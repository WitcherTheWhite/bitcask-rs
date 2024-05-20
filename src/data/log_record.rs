use std::u8;

use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

// 数据位置索引信息，描述数据存储的位置
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

// LogRecord 写入到数据文件的记录
#[derive(Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    // 对 LogRecord 编码
    pub fn encode(&self) -> Vec<u8> {
        let (enc_buf, _) = self.encoder_and_get_crc();
        enc_buf
    }

    pub fn get_crc(&self) -> u32 {
        let (_, crc) = self.encoder_and_get_crc();
        crc
    }

    fn encoder_and_get_crc(&self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::new();
        buf.reserve(self.encoded_length());

        buf.put_u8(self.rec_type as u8);

        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();

        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);

        (buf.to_vec(), crc)
    }

    // LogRecord 编码后的长度
    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LogRecordType {
    NOAMAL = 1,       // 正常写入的数据
    DELETED = 2,      // 删除数据的标记，墓碑值
    TXNFINISHED = 3, // 标记事务完成的数据
}

impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NOAMAL,
            2 => LogRecordType::DELETED,
            3 => LogRecordType::TXNFINISHED,
            _ => panic!("unknown log record type"),
        }
    }
}

// 读取 LogRecord 的信息，包括数据大小
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64,
}

// 事务数据的信息
pub struct TransactionLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) pos: LogRecordPos,
}

// LogRecord header 部分最大长度
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode() {
        // 正常 LogRecord 编码
        let rec1 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "hsy".as_bytes().to_vec(),
            rec_type: LogRecordType::NOAMAL,
        };
        let enc1 = rec1.encode();
        assert!(enc1.len() > 12);

        // value 为空
        let rec2 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::NOAMAL,
        };
        let enc2 = rec2.encode();
        assert!(enc2.len() > 9);

        // Deleted
        let rec3 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETED,
        };
        let enc3 = rec3.encode();
        assert!(enc3.len() > 9)
    }
}
