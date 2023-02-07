use std::fs;
use std::fs::File;
use std::io::{Write};
use std::path::{Path, PathBuf};

use crate::wal::write_ahead_log_entry::{WriteAheadLogEntries, WriteAheadLogEntry};

pub struct WriteAheadLog {
    file: File,
    path: PathBuf,
}

impl WriteAheadLog {
    pub fn init_or_panic(path: PathBuf) -> WriteAheadLog {
        match fs::metadata(path.as_path()) {
            Ok(_) => WriteAheadLog { file: Self::open_file_or_panic(&path), path },
            Err(_) => WriteAheadLog { file: Self::create_file_or_panic(&path), path }
        }
    }

    fn open_file_or_panic<F>(path: &F) -> File
        where F: AsRef<Path> {
        return match File::options().append(true).read(true).open(&path) {
            Ok(file) => file,
            Err(err) => panic!("error while opening write-ahead log on path {:?} with {:?}", path.as_ref(), err),
        };
    }

    fn create_file_or_panic<F>(path: &F) -> File
        where F: AsRef<Path> {
        return match File::create(path) {
            Ok(file) => file,
            Err(err) => panic!("error while creating write-ahead log on path {:?} with {:?}", path.as_ref(), err),
        };
    }

    pub fn append(&mut self, bytes: Vec<u8>, generation: u64) {
        let entry = WriteAheadLogEntry::new(bytes, generation);
        let _ = &self.file.write(&entry.serialize()).unwrap();
    }

    fn read_all(&mut self) -> Option<WriteAheadLogEntries> {
        let mut f = File::open(&self.path).unwrap();
        return WriteAheadLogEntries::deserialize_entries(&mut f);
    }
}

#[cfg(all(test, feature="test_type_unit"))]
mod tests {
    use std::env::temp_dir;
    use std::fs::remove_file;

    use crate::wal::write_ahead_log::WriteAheadLog;

    #[test]
    fn append_an_entry_to_write_ahead_log() {
        let content = String::from("write-ahead log").as_bytes().to_vec();
        let write_ahead_log_path = temp_dir().join("wal_test_single.log");
        let write_ahead_log_path_clone = write_ahead_log_path.clone();

        let mut write_ahead_log = WriteAheadLog::init_or_panic(write_ahead_log_path);
        write_ahead_log.append(content, 1);

        let entries = write_ahead_log.read_all().unwrap();
        let _ = remove_file(write_ahead_log_path_clone.as_path());

        let entry = entries.entry_at_index(0).unwrap();
        assert_eq!(1, entry.get_generation());

        let expected_content = String::from("write-ahead log").as_bytes().to_vec();
        assert_eq!(&expected_content, entry.get_bytes());
    }

    #[test]
    fn append_2_entries_to_write_ahead_log() {
        let write_ahead_log_path = temp_dir().join("wal_test_multiple.log");
        let write_ahead_log_path_clone = write_ahead_log_path.clone();

        let mut write_ahead_log = WriteAheadLog::init_or_panic(write_ahead_log_path);

        let content = String::from("write-ahead log").as_bytes().to_vec();
        write_ahead_log.append(content, 1);

        let content = String::from("raft log").as_bytes().to_vec();
        write_ahead_log.append(content, 2);

        let entries = write_ahead_log.read_all().unwrap();
        let _ = remove_file(write_ahead_log_path_clone.as_path());

        let entry = entries.entry_at_index(0).unwrap();
        assert_eq!(1, entry.get_generation());

        let expected_content = String::from("write-ahead log").as_bytes().to_vec();
        assert_eq!(&expected_content, entry.get_bytes());

        let entry = entries.entry_at_index(1).unwrap();
        assert_eq!(2, entry.get_generation());

        let expected_content = String::from("raft log").as_bytes().to_vec();
        assert_eq!(&expected_content, entry.get_bytes());
    }
}