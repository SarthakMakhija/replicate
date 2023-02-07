use std::io::ErrorKind::UnexpectedEof;
use std::io::Read;
use bincode::ErrorKind::Io;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WriteAheadLogEntry {
    bytes: Vec<u8>,
    generation: u64,
}

impl WriteAheadLogEntry {
    pub(crate) fn new(bytes: Vec<u8>, generation: u64) -> Self {
        return WriteAheadLogEntry {
            bytes,
            generation,
        };
    }

    pub(crate) fn serialize(&self) -> Vec<u8> {
        return bincode::serialize(&self).unwrap();
    }

    pub(crate) fn get_generation(&self) -> u64 {
        return self.generation;
    }

    pub(crate) fn get_bytes(&self) -> &[u8] {
        return &self.bytes;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WriteAheadLogEntries {
    pub(crate) entries: Vec<WriteAheadLogEntry>,
}

impl WriteAheadLogEntries {
    pub(crate) fn deserialize_entries<R: Read>(reader: &mut R) -> Option<WriteAheadLogEntries> {
        let mut entries = vec![];
        loop {
            match bincode::deserialize_from(&mut *reader) {
                Ok(entry) => entries.push(entry),
                Err(err_kind) => {
                    if let Io(ref err) = *err_kind {
                        if err.kind() == UnexpectedEof {
                            break;
                        }
                    }
                    return None;
                }
            }
        }
        return Some(WriteAheadLogEntries { entries });
    }

    pub(crate) fn entry_at_index(&self, index: usize) -> Option<&WriteAheadLogEntry> {
        return self.entries.get(index);
    }
}
