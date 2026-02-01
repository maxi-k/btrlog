use std::{net::SocketAddr, pin::Pin, rc::Rc, sync::Arc};

use crate::{
    client::{ForCluster, journal_state::WalSlotGuard, quorum::RetryStatus},
    config::{ClientConfig, IOConfig},
    io::buffer::IoBuffer,
    node::{cluster::ClusterInfo, health::ClusterHealth},
    types::{id::JournalId, message::*, packet::ResponsePacket},
};

use super::{
    ClientExecutor, ClusterClientView, JournalClientState, WalPosition,
    journal_state::WalRequestQueue,
    quorum::{JournalDriverConfig, JournalQuorumDriver, RetrySpec, bind_retrying_client},
};

pub trait GlobalClientState {
    fn io_config(&self) -> &'static IOConfig;
    fn client_config(&self) -> &'static ClientConfig;
    fn driver_config(&self) -> JournalDriverConfig;
    fn cluster_view(&self) -> &ClusterClientView;
    fn cluster_info(&self) -> &ClusterInfo;
    fn cluster_health(&self) -> &ClusterHealth;
    // retry specs
    fn cluster_state_retry_spec(&self) -> RetrySpec;
    fn journal_creation_retry_spec(&self) -> RetrySpec;
    fn journal_append_retry_spec(&self) -> RetrySpec;
}

impl<T: GlobalClientState> GlobalClientState for Arc<T> {
    fn io_config(&self) -> &'static IOConfig {
        T::io_config(self)
    }

    fn client_config(&self) -> &'static ClientConfig {
        T::client_config(self)
    }

    fn driver_config(&self) -> JournalDriverConfig {
        T::driver_config(self)
    }

    fn cluster_view(&self) -> &ClusterClientView {
        T::cluster_view(self)
    }

    fn cluster_info(&self) -> &ClusterInfo {
        T::cluster_info(self)
    }

    fn cluster_health(&self) -> &ClusterHealth {
        T::cluster_health(self)
    }

    fn cluster_state_retry_spec(&self) -> RetrySpec {
        T::cluster_state_retry_spec(self)
    }

    fn journal_creation_retry_spec(&self) -> RetrySpec {
        T::journal_creation_retry_spec(self)
    }

    fn journal_append_retry_spec(&self) -> RetrySpec {
        T::journal_append_retry_spec(self)
    }
}

pub struct ClientWorker<GlobalState> {
    cfg: &'static ClientConfig,
    global: GlobalState,
    exec: Rc<ClientExecutor>,
    driver: Rc<JournalQuorumDriver>,
}

impl<GlobalState: GlobalClientState> ClientWorker<GlobalState> {
    pub fn new(global: GlobalState, exec: Rc<ClientExecutor>) -> std::io::Result<Self> {
        let cfg = global.client_config();
        let local_ip = global.cluster_health().ip();
        let driver = bind_retrying_client((local_ip, 0u16), global.driver_config())?;
        Ok(Self {
            cfg,
            global,
            exec,
            driver,
        })
    }

    pub fn global(&self) -> &GlobalState {
        &self.global
    }

    pub async fn create_journal(&self, journal_id: JournalId) -> JournalClientState {
        let cluster = self.global.cluster_view();
        let journal_retry_spec = self.global.journal_creation_retry_spec();
        #[cfg(feature = "env-logger")]
        log::debug!("generated journal id {:?} on thread {:?}", journal_id, std::thread::current().id());
        let req = CreateJournalRequest { id: journal_id.into() };
        let (replies, remotes) = self
            .driver
            .retrying_quorum_send_recv(JournalRequest::CreateJournal(req), &journal_retry_spec, cluster.clone(), |msg, _from| {
                match &msg.request {
                    &JournalResponse::CreateJournal(ref create_resp) => match create_resp {
                        CreateJournalResponse::Ok | CreateJournalResponse::JournalAlreadyExists => RetryStatus::Ok,
                        CreateJournalResponse::BlobStoreError(_) => RetryStatus::Retry,
                    },
                    x => RetryStatus::Abort(format!("unexpected response to CreateJournal: {:?}", x)),
                }
            })
            .await
            .expect("error creating journals");
        // let req_size = retrying_stream.get_message_bytes();
        // let start_time = Instant::now();
        // let majority_latency = start_time.elapsed();
        // let retries = retrying_stream.get_executed_retries();
        // stats.record_request(majority_latency, req_size, retries);
        //
        let reply_to = replies
            .into_iter()
            .zip(remotes.into_iter())
            .map(|(resp, addr)| match resp {
                Some(ResponsePacket {
                    header,
                    request: JournalResponse::CreateJournal(create_resp),
                }) => {
                    match create_resp {
                        CreateJournalResponse::Ok => {
                            #[cfg(feature = "env-logger")]
                            log::trace!("created journal on {:?}", addr)
                        }
                        CreateJournalResponse::JournalAlreadyExists => {
                            #[cfg(feature = "env-logger")]
                            log::trace!("journal already exists on {:?}", addr);
                        },
                        CreateJournalResponse::BlobStoreError(_e) => {
                            #[cfg(feature = "env-logger")]
                            log::warn!("blobstore error creating journal on {:?}: {:?}", addr, _e)
                        }
                    };
                    header.reply_to
                }
                None => {
                    #[cfg(feature = "env-logger")]
                    log::trace!("no response received from {:?}", addr);
                    addr.port()
                }
                e => panic!("unexpected response creating journal: {:?}", e),
            })
            .collect::<ForCluster<_>>();
        JournalClientState::new_empty(journal_id, reply_to, self.global.io_config().lsn_window as u64)
    }

    pub async fn execute_add_entry_request(
        &self,
        mut seq_guard: Pin<&mut WalSlotGuard<'_>>,
        wal_position: WalPosition,
        entry: AddEntryRequest,
    ) -> WalPosition {
        let cluster = self.global.cluster_view();
        let retry_spec = self.global.journal_append_retry_spec();
        // broadcast append request with retry logic
        let (replies, remotes) = self
            .driver
            .retrying_quorum_send_recv(
                JournalRequest::AddEntry(entry.clone()),
                &retry_spec,
                cluster
                    .iter()
                    .zip(seq_guard.state().reply_to_iter())
                    .map(|(addr, port)| SocketAddr::from((addr.ip(), *port))),
                |msg, _from| {
                    match &msg.request {
                        &JournalResponse::AddEntry(ref add_resp) => {
                            match seq_guard.check(&add_resp) {
                                //
                                true => RetryStatus::Ok,
                                false => RetryStatus::Retry,
                            }
                        }
                        x => RetryStatus::Abort(format!("unexpected response to AddEntry: {:?}", x)),
                    }
                },
            )
            .await
            .expect(format!("error receiving replies for {} - {}", entry.id, entry.lsn).as_str());

        let mut any_response: Option<&AddEntryResponse> = None;
        for (idx, (resp, _addr)) in replies.iter().zip(remotes.into_iter()).enumerate() {
            match resp {
                Some(ResponsePacket {
                    header: h,
                    request: JournalResponse::AddEntry(add_resp),
                }) => {
                    debug_assert_eq!(add_resp.lsn, wal_position.lsn);
                    seq_guard.set_reply_to(idx, h.reply_to);
                    any_response = Some(&add_resp);
                }
                None => {
                    #[cfg(feature = "env-logger")]
                    log::trace!(
                        "no response received from {:?} for (AddEntry :lsn {} :offset {})",
                        _addr,
                        entry.lsn,
                        entry.offset
                    )
                }
                x => panic!("unexpected response: {:?} for request {:?}", x, entry),
            }
        }
        match any_response {
            Some(resp) => seq_guard.ensure_completion_sequence(resp).await,
            None => unreachable!("received responses, but no response set"),
        };
        wal_position
    }

    pub async fn add_entry_to_journal(
        &self,
        state: &mut JournalClientState,
        queue: &WalRequestQueue,
        payload: &[u8],
    ) -> WalPosition {
        let mut seq_guard = std::pin::pin!(WalSlotGuard::new(state, queue));
        let wal_position = seq_guard.get_wal_slot(payload.len() as u64).expect("error getting wal slot");
        let entry = seq_guard.make_request(&wal_position, payload);
        self.execute_add_entry_request(seq_guard, wal_position, entry).await
    }
}
