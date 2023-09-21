#[derive(Debug)]
pub struct Message {
    pub sender_id: u32,
    pub chunk_id: u32,
    pub last_chunk: bool,
    pub data: Vec<u8>,
}
