mod client_runtime;
pub mod client_worker;
mod journal_state;
pub mod quorum;
pub mod stats;

pub use client_runtime::{ClientExecutor, ClientRuntime, client_exec, client_rt};
pub use journal_state::{JournalClientState, WalPosition, WalRequestQueue, WalSlotGuard};


pub type ForCluster<T> = smallvec::SmallVec<[T; 8]>; // clusters should mostly be <= 8
pub type ClusterClientView = ForCluster<std::net::SocketAddr>;

pub trait ClusterUtils {
    fn node_count(&self) -> usize;
    fn majority(&self) -> usize;
}

impl ClusterUtils for ClusterClientView {
    fn node_count(&self) -> usize {
        self.len()
    }

    fn majority(&self) -> usize {
        (self.node_count() / 2) + 1
    }
}
