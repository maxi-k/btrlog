use super::{message::*, wire::auto::*};

// Messages that aren't sent as part of a persistent connection
// additionally need an id to correlate requests with responses.
// The 'reply_to' field is used to for load balancing & sending
// packets to the correct port <-> thread immediately.
#[derive(Encode, Decode)]
#[apply(on_wire)]
pub struct PacketHeader {
    pub msg_id: u64,
    pub reply_to: u16,
}

#[derive(Encode, Decode)]
#[apply(on_wire)]
pub struct RequestPacket {
    pub header: PacketHeader,
    pub request: JournalRequest,
}

#[derive(Encode, Decode)]
#[apply(on_wire)]
pub struct ResponsePacket {
    pub header: PacketHeader,
    pub request: JournalResponse,
}
