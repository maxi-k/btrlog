use super::{id::Epoch, wire::auto::*};

////////////////////////////////////////////////////////////////////////////////
//  journal push

#[derive(Encode, Decode)]
#[apply(on_wire)]
pub enum JournalPushError {
    NoSuchJournal,
    NotEnoughFreeSpace,
    LsnOutsideOfRange,
    LsnAlreadySeen,
    EpochTooSmall,
    EpochTooLarge,
    OffsetTooSmall,
    OffsetTooLarge,
}

////////////////////////////////////////////////////////////////////////////////
//  blob store

#[derive(Encode, Decode, derive_more::Display, thiserror::Error)]
#[apply(on_wire)]
pub enum BlobStoreWriteError {
    ObjectNameCollision,
    BlobStoreNotReachable,
    Unknown,
}

#[derive(Encode, Decode, derive_more::Display, thiserror::Error)]
#[apply(on_wire)]
pub enum BlobStoreBucketError {
    BucketNameCollision,
    BlobStoreNotReachable,
    BucketContention,
    Unknown,
}

#[derive(Encode, Decode, derive_more::Display, thiserror::Error)]
#[apply(on_wire)]
pub enum BlobStoreReadError {
    NoSuchObject,
    BlobStoreNotReachable,
    BrokenByteStream,
    Unknown,
}

#[derive(Debug, derive_more::Display, thiserror::Error)]
#[apply(msg_enum_from)]
pub enum JournalPageFetchError {
    // PageNotFound, == NoSuchObject
    BlobStoreError(BlobStoreReadError),
}

////////////////////////////////////////////////////////////////////////////////
//  fetch entry

#[derive(Encode, Decode)]
#[apply(on_wire)]
#[apply(msg_enum_from)]
pub enum FetchEntryError {
    JournalNotFound,
    LSNNotFound,
    LSNTooOld,
    WrongEpoch(Epoch),
}

////////////////////////////////////////////////////////////////////////////////
//  fetch metadata

#[derive(Encode, Decode)]
#[apply(on_wire)]
#[apply(msg_enum_from)]
pub enum JournalMetadataError {
    JournalNotFound,
}
