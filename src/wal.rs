// Implements a simple WAL for the database's memtable.

use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{BufReader, ErrorKind, Write},
    path::Path,
};

use crate::{
    kvrecord::{KVRecord, WriteKVRecord, OP_SET},
    ToyKVError,
};

pub(crate) struct WAL<'a> {
    d: &'a Path,
}

pub(crate) fn new(d: &Path) -> WAL {
    WAL { d }
}

// TODO
// We should have a state machine here. First you need to replay() the
// WAL both to read through the data to check it's valid and find the
// right seq for appending, and also to reload the database memtable.
// Then, and only then, should you be able to call write().

impl<'a> WAL<'a> {
    /// Replays the WAL into a memtable. Call this first.
    pub(crate) fn replay(&mut self) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, ToyKVError> {
        let wal_path = self.d.join("db.wal");

        let mut memtable = BTreeMap::new();

        let file = match OpenOptions::new().read(true).open(wal_path) {
            Ok(it) => it,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(memtable),
            Err(e) => return Err(e.into()),
        };

        // A somewhat large buffer as we expect these files to be quite large.
        let mut bytes = BufReader::with_capacity(256 * 1024, file);

        loop {
            let rec = KVRecord::read_one(&mut bytes)?;
            match rec {
                Some(wr) => memtable.insert(wr.key, wr.value),
                None => break, // assume we hit the end of the WAL file
            };
        }

        Ok(memtable)
    }

    /// Appends entry to WAL
    pub(crate) fn write(&mut self, key: &[u8], value: &[u8]) -> Result<(), ToyKVError> {
        let seq = 1u32; // TODO implement sequence numbers for records

        // TODO hold the file open in the WAL struct rather than opening
        // for every write.

        let wal_path = self.d.join("db.wal");
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(wal_path)?;

        // Create our record and attempt to write
        // it out in one go.
        let op = OP_SET;
        let wr = WriteKVRecord {
            seq,
            op,
            key,
            value,
        };
        file.write_all(&wr.serialize())?;
        file.sync_all()?;

        Ok(())
    }
}
