use crate::trace::MetricMap;

use crate::trace::RequestTracer;

// XXX  clean up the error format / packaging
use super::{error::*, id::*, wire::auto::*};
use std::fmt;

#[allow(async_fn_in_trait)]
pub trait JournalMessageReceiver {
    type Metrics: MetricMap;
    async fn accept(&self, req: &JournalRequest) -> JournalResponse {
        let tracer = crate::trace::RequestTracer::default();
        self.accept_tracing(req, &tracer).await
    }
    async fn accept_tracing(&self, req: &JournalRequest, tracer: &crate::trace::RequestTracer) -> JournalResponse;
    fn take_stats(&self) -> Self::Metrics;
    fn journal_count(&self) -> usize;
}

////////////////////////////////////////////////////////////////////////////////
//  generic request

#[derive(Encode, Decode)]
#[apply(on_wire)]
#[apply(msg_enum_from)]
pub enum JournalRequest {
    CreateJournal(CreateJournalRequest),
    AddEntry(AddEntryRequest),
    FetchEntry(FetchEntryRequest),
    FetchMeta(JournalMetadataRequest),
    //
    JournalHeartbeat(WireJournalId),
    Heartbeat,
    Shutdown,
}

impl JournalRequest {
    pub fn journal_id(&self) -> Option<JournalId> {
        use JournalRequest::*;
        match self {
            CreateJournal(create_journal) => Some(create_journal.id.into()),
            AddEntry(add_entry) => Some(add_entry.id.into()),
            FetchEntry(fetch_entry) => Some(fetch_entry.id.into()),
            FetchMeta(journal_metadata) => Some(journal_metadata.id.into()),
            JournalHeartbeat(id) => Some((*id).into()),
            Heartbeat => None,
            Shutdown => None,
        }
    }
}

impl fmt::Display for JournalRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalRequest::CreateJournal(r) => f.debug_struct("CreateJournal").field("id", &r.id).finish(),
            JournalRequest::AddEntry(r) => f
                .debug_struct("AddEntry")
                .field("journal", &r.id)
                .field("lsn", &r.lsn)
                .field("offset", &r.offset)
                .field("epoch", &r.epoch)
                .field("data", &r.payload.len())
                .finish(),
            JournalRequest::FetchEntry(r) => f.debug_struct("FetchEntry").field("journal", &r.id).field("lsn", &r.lsn).finish(),
            JournalRequest::FetchMeta(r) => f.debug_struct("FetchMeta").field("journal", &r.id).finish(),
            JournalRequest::JournalHeartbeat(id) => f.debug_struct("JournalHeartbeat").field("journal", &id).finish(),
            JournalRequest::Heartbeat => f.debug_struct("Heartbeat").finish(),
            JournalRequest::Shutdown => f.debug_struct("Shutdown").finish(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
//  generic response

#[derive(Encode, Decode)]
#[apply(on_wire)]
#[apply(msg_enum_from)]
pub enum JournalResponse {
    CreateJournal(CreateJournalResponse),
    AddEntry(AddEntryResponse),
    FetchEntry(FetchEntryResponse),
    FetchMeta(JournalMetadataResponse),
    //
    JournalHeartbeat(WireJournalId),
    Heartbeat,
    ShutdownSucessful,
    ShutdownCancelled,
}

////////////////////////////////////////////////////////////////////////////////
//  heartbeats

#[derive(Encode, Decode, Copy)]
#[apply(on_wire)]
pub enum NodeState {
    Init,
    Alive,
    Ready,
    Stopping,
}

#[derive(Encode, Decode, Copy)]
#[apply(on_wire)]
pub struct HeartbeatMessage {
    pub node_id: usize,
    pub alive_nodes: usize,
    pub state: NodeState,
    pub _padding: [u8; 8],
}

////////////////////////////////////////////////////////////////////////////////
//  log creation
#[apply(on_wire)]
#[derive(Encode, Decode)]
pub struct CreateJournalRequest {
    pub id: WireJournalId,
}

#[derive(Encode, Decode)]
#[apply(on_wire)]
#[apply(msg_enum_from)]
pub enum CreateJournalResponse {
    Ok,
    JournalAlreadyExists,
    BlobStoreError(BlobStoreBucketError),
}

////////////////////////////////////////////////////////////////////////////////
//  add entry
// TODO support arbitrary-length payloads
#[apply(on_wire, max_size = 65535)]
#[derive(Encode, Decode)]
pub struct AddEntryRequest {
    pub id: WireJournalId,
    pub epoch: Epoch,
    pub lsn: LSN,
    pub offset: LogicalJournalOffset,
    pub payload: Vec<u8>,
}

#[apply(on_wire)]
#[derive(Encode, Decode)]
pub struct AddEntryResponse {
    pub id: WireJournalId,
    pub epoch: Epoch,
    pub lsn: LSN,
    pub result: Result<LogicalJournalOffset, JournalPushError>,
    pub tracer: RequestTracer
}

////////////////////////////////////////////////////////////////////////////////
//  fetch entry

#[apply(on_wire)]
#[derive(Encode, Decode)]
pub struct FetchEntryRequest {
    pub id: WireJournalId,
    pub epoch: Epoch,
    pub lsn: LSN,
}

#[derive(Encode, Decode)]
#[apply(on_wire)]
pub struct FetchEntrySuccess {
    pub offset: LogicalJournalOffset,
    pub data: Vec<u8>,
}

#[derive(Encode, Decode)]
#[apply(on_wire)]
pub struct FetchEntryResponse {
    pub result: Result<FetchEntrySuccess, FetchEntryError>,
}

////////////////////////////////////////////////////////////////////////////////
//  inspect journal

#[apply(on_wire)]
#[derive(Encode, Decode)]
pub struct JournalMetadataRequest {
    pub id: WireJournalId,
}

#[derive(Encode, Decode)]
#[apply(on_wire)]
pub struct JournalMetadataResponse {
    pub result: Result<JournalMetadata, JournalMetadataError>,
}

#[derive(Encode, Decode)]
#[apply(on_wire)]
pub struct JournalMetadata {
    pub id: WireJournalId,
    pub max_known_lsn: LSN,
    pub max_known_offset: LogicalJournalOffset,
    pub current_epoch: Epoch,
}

////////////////////////////////////////////////////////////////////////////////
//  flush journal

#[derive(Encode, Decode)]
#[apply(on_wire, unbounded)]
#[apply(msg_enum_from)]
pub enum FlushResult {
    Success,
    BlobStoreError(BlobStoreWriteError),
    IOError(String),
}

////////////////////////////////////////////////////////////////////////////////
//  tests

#[cfg(test)]
mod tests {
    use crate::dst::RandomDST as _;

    use super::*;
    use more_asserts::*;

    #[test]
    fn serialize_entry_request() {
        assert_eq!(AddEntryRequest::WIRE_SIZE_BOUND, 65535);
        let mut buf = [0; AddEntryRequest::WIRE_SIZE_BOUND]; // From PackedSize
        let msg = AddEntryRequest {
            id: JournalId::dst_random().into(),
            epoch: 42,
            lsn: LSN::dst_random(),
            offset: 0,
            payload: vec![1, 2, 3],
        };
        // encode
        let enc_size = AddEntryRequest::encode_into(msg.clone(), &mut buf).unwrap();
        assert_le!(enc_size, msg.est_wire_size());
        // decode
        let (decoded, size) = AddEntryRequest::decode_from(&mut buf).unwrap();
        assert_le!(size, msg.est_wire_size());
        // should be equal
        assert_eq!(msg, decoded);
    }

    #[test]
    fn serialize_generic_request() {
        // small requests don't take up the size of large requests
        {
            let mut buf = [0; JournalRequest::WIRE_SIZE_BOUND];
            let msg = JournalRequest::Heartbeat;
            // encode
            let enc_size = msg.clone().encode_into(&mut buf).unwrap();
            assert_le!(enc_size, 4);
            // decode
            let (decoded, size) = JournalRequest::decode_from(&mut buf).unwrap();
            assert_le!(size, 4);
            // should be equal
            assert_eq!(msg, decoded);
        }
        // overhead is smaller than 4 bytes
        {
            let mut buf1 = [0; AddEntryRequest::WIRE_SIZE_BOUND];
            let mut buf2 = [0; JournalRequest::WIRE_SIZE_BOUND];
            let msg = AddEntryRequest {
                id: JournalId::dst_random().into(),
                epoch: 42,
                lsn: LSN::dst_random(),
                offset: 0,
                payload: vec![1, 2, 3],
            };
            // encode
            let enc_size_raw = msg.clone().encode_into(&mut buf1).unwrap();
            let enc_size_wrapped = JournalRequest::AddEntry(msg.clone()).encode_into(&mut buf2).unwrap();
            assert_le!(enc_size_wrapped, 4 + enc_size_raw);
            // decode
            let (decoded_raw, size_raw) = AddEntryRequest::decode_from(&mut buf1).unwrap();
            let (decoded_wrapped, size_wrapped) = JournalRequest::decode_from(&mut buf2).unwrap();
            assert_le!(size_wrapped, 4 + size_raw);
            // should be equal
            assert_eq!(decoded_wrapped, JournalRequest::AddEntry(decoded_raw));
        }
    }

    #[test]
    fn serialize_fetch_request() {
        let mut buf = [0; FetchEntryRequest::WIRE_SIZE_BOUND];
        let msg = FetchEntryRequest {
            id: JournalId::dst_random().into(),
            epoch: 42,
            lsn: LSN::dst_random(),
        };
        // encode
        let enc_size = FetchEntryRequest::encode_into(msg.clone(), &mut buf).unwrap();
        assert_le!(enc_size, msg.est_wire_size());
        // decode
        let (decoded, size) = FetchEntryRequest::decode_from(&mut buf).unwrap();
        assert_le!(size, msg.est_wire_size());
        // should be equal
        assert_eq!(msg, decoded);
    }
}
