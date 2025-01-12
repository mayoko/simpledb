use crate::buffer::buffer;
use crate::file::{blockid, file_manager, page};
use crate::log::log_manager;

const MAX_TIME: u32 = 10_000; // 10 seconds

pub struct BufferManager<'a> {
    buffer_pool: Vec<buffer::Buffer<'a>>,
    num_available: usize,
}
