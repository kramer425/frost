use serde;
use serde::de;
use serde_rosmsg;

use crate::errors::Error;
use crate::{ChunkHeaderLoc, DecompressedBag};

pub trait Msg {}

pub struct MessageView<'a> {
    pub topic: &'a str,
    pub(crate) bag: &'a DecompressedBag,
    pub(crate) chunk_loc: ChunkHeaderLoc,
    pub(crate) start_index: usize,
    pub(crate) end_index: usize,
}

impl<'a> MessageView<'a> {
    /// Returns the raw bytes of the entire Chunk that holds the message
    fn chunk_bytes(&self) -> &'a [u8] {
        self.bag
            .chunk_bytes
            .get(&self.chunk_loc)
            .map(|vec| vec.as_slice())
            .expect("this function is only possible to be called on a bag with chunks populated")
    }

    /// Returns the raw bytes of the entire ROS message
    pub fn raw_bytes(&self) -> Result<&'a [u8], Error> {
        Ok(&self.chunk_bytes()[self.start_index..self.end_index])
    }

    /// Size of the message, in bytes
    pub fn size(&self) -> u64 {
        (self.end_index - self.start_index + 1) as u64
    }

    /// Turns a `MessageView` into a Rust struct
    pub fn instantiate<'de, T>(&self) -> Result<T, Error>
    where
        T: Msg,
        T: de::Deserialize<'de>,
    {
        serde_rosmsg::from_slice(self.raw_bytes()?).map_err(|e| e.into())
    }
}
