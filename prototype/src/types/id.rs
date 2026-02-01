use crate::dst::*;
use std::{str::FromStr, sync::atomic::AtomicUsize};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////
//  Journal ID
pub type JournalId = Uuid;
pub trait RadixPrefix {
    fn radix_bits(&self, idx: usize) -> u8;
    fn partition(&self) -> usize {
        self.radix_bits(0) as usize
    }
}

impl RadixPrefix for JournalId {
    fn radix_bits(&self, idx: usize) -> u8 {
        self.as_bytes()[idx]
    }
}

// Type wrapper that we own for implementing foreign traits
#[repr(transparent)]
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, derive_more::Display)]
pub struct WireJournalId(JournalId);
impl std::ops::Deref for WireJournalId {
    type Target = JournalId;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Into<JournalId> for WireJournalId {
    fn into(self) -> JournalId {
        self.0
    }
}
impl Into<WireJournalId> for JournalId {
    fn into(self) -> WireJournalId {
        WireJournalId(self)
    }
}

impl FromStr for WireJournalId {
    type Err = <JournalId as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JournalId::from_str(s).map(|r| WireJournalId(r))
    }
}

////////////////////////////////////////////////////////////////////////////////
//  LSN & Offset
pub type LSN = u64;
pub const INVALID_LSN: LSN = LSN::max_value();
pub type AtomicLSN = AtomicUsize;

pub type JournalOffset = u32;
pub type LogicalJournalOffset = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageIdentifier(pub Epoch, pub LSN, pub LogicalJournalOffset);
impl PageIdentifier {
    pub fn split(&self) -> (Epoch, LSN, LogicalJournalOffset) {
        (self.0, self.1, self.2)
    }
}

////////////////////////////////////////////////////////////////////////////////
//  Epoch
pub type Epoch = u64;

////////////////////////////////////////////////////////////////////////////////
//  DST Support
impl RandomDST for JournalId {
    fn dst_map(rng: &mut DstRng) -> Self {
        let mut bytes = [0u8; 16];
        rng.fill_bytes(&mut bytes);
        uuid::Builder::from_random_bytes(bytes).into_uuid()
    }
}
