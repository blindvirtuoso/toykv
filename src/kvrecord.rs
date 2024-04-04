/*
KVRecord and KVWriteRecord allow writing keys and values to files.

The WAL is a simple sequence of records written to a file. The main
interesting item is the seq, which is expected to incrase by 1 with
each item. A u32 allows for 4,294,967,295 records. We should've
flushed the associated memtable to disk long before we get that
far.

0       1         5       6            8              12    16            N
| magic | u32 seq | u8 op | u16 keylen | u32 valuelen | pad | key | value |
  ---------------------------------------------------------   ---   -----
                 16 bytes header                               |      |
                                                      keylen bytes    |
                                                                      |
                                                           valuelen bytes

Valid `op` values:

- 1: SET


*/

use std::io::{Error, Read};

const MAGIC: u8 = b'w';
const PAD: u32 = 0u32;

pub(crate) const OP_SET: u8 = 1u8;

#[derive(Debug)]
/// A read-optimised version of KVRecord. It owns the key and
/// value data.
pub(crate) struct KVRecord {
    // magic: u8,
    // pub(crate) seq: u32,
    // pub(crate) op: u8,
    // keylen: u16,
    // valuelen: u32,
    // _pad: u32,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl KVRecord {
    /// Read a single WAL record from a WAL file (or other Read struct).
    pub(crate) fn read_one<T: Read>(r: &mut T) -> Result<Option<KVRecord>, Error> {
        let mut header = [0u8; 16];
        let n = r.read(&mut header)?;
        if n < 16 {
            // Is this really only Ok if we read zero?
            // 0 < n < 16 probably actually means a corrupt file.
            return Ok(None);
        }

        // This might be clearer using byteorder and a reader
        let magic = header[0];
        assert_eq!(magic, MAGIC, "Unexpected magic byte");
        // let seq = u32::from_be_bytes(header[1..5].try_into().unwrap());
        let op = header[5];
        assert_eq!(op, OP_SET, "Unexpected op code");
        let keylen = u16::from_be_bytes(header[6..8].try_into().unwrap());
        let valuelen = u32::from_be_bytes(header[8..12].try_into().unwrap());
        let _pad = u32::from_be_bytes(header[12..16].try_into().unwrap());
        assert_eq!(_pad, PAD, "Unexpected padding of non-zero");

        let mut key = Vec::with_capacity(keylen as usize);
        r.by_ref().take(keylen as u64).read_to_end(&mut key)?;
        let mut value = Vec::with_capacity(valuelen as usize);
        r.by_ref().take(valuelen as u64).read_to_end(&mut value)?;

        let wr = KVRecord {
            // seq,
            // op,
            key,
            value,
        };

        println!("Read WAL record: {:?}", wr);

        Ok(Some(wr))
    }
}

#[derive(Debug)]
/// A write-optimised version of KV record. It uses slices for key and value to
/// avoid extra copies.
pub(crate) struct WriteKVRecord<'a> {
    // magic: u8,
    pub(crate) seq: u32,
    pub(crate) op: u8,
    // keylen: u16,
    // valuelen: u32,
    // _pad: u32,
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
}
impl<'a> WriteKVRecord<'a> {
    /// Create the serialised form of the WriteKVRecord.
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::<u8>::new();
        buf.push(MAGIC);
        buf.extend(self.seq.to_be_bytes());
        buf.push(self.op);
        buf.extend((self.key.len() as u16).to_be_bytes());
        buf.extend((self.value.len() as u32).to_be_bytes());
        buf.extend(PAD.to_be_bytes());
        buf.extend(self.key);
        buf.extend(self.value);
        buf
    }
}
