use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};

use crate::{
    data::log_record::{LogRecord, LogRecordType},
    db::Engine,
    errors::Errors,
    options::{IndexType, WriteBatchOptions},
};

const TXN_FIN_KEY: &[u8] = "txn-fin".as_bytes();
pub(crate) const NON_TXN_SEQ_NO: usize = 0;

/// 批量写操作，保证原子性
pub struct WriteBatch<'a> {
    prending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    engine: &'a Engine,
    options: WriteBatchOptions,
}

impl Engine {
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch, Errors> {
        if self.options.index_type == IndexType::BPlusTree && !self.seq_file_exists && !self.is_initial {
            return Err(Errors::UnableToUseWriteBatch);
        }
        Ok(WriteBatch {
            prending_writes: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options,
        })
    }
}

impl WriteBatch<'_> {
    /// 批量操作写数据
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<(), Errors> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NOAMAL,
        };

        let mut pending_writes = self.prending_writes.lock();
        pending_writes.insert(key.to_vec(), record);

        Ok(())
    }

    /// 批量操作删除数据
    pub fn delete(&self, key: Bytes) -> Result<(), Errors> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let mut pending_writes = self.prending_writes.lock();

        // key 不存在直接返回
        let pos = self.engine.index.get(key.to_vec());
        if pos.is_none() {
            if pending_writes.contains_key(&key.to_vec()) {
                pending_writes.remove(&key.to_vec());
            }
        }

        let record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETED,
        };
        pending_writes.insert(key.to_vec(), record);

        Ok(())
    }

    /// 提交数据，将数据写到文件，更新内存索引
    pub fn commit(&self) -> Result<(), Errors> {
        let mut pending_writes = self.prending_writes.lock();
        if pending_writes.len() == 0 {
            return Ok(());
        }
        if pending_writes.len() > self.options.max_batch_num as usize {
            return Err(Errors::ExceedMaxBatchNum);
        }

        // 加锁保证事务提交串行化
        let _lock = self.engine.batch_commit_lock.lock();

        // 获取全局事务序列号
        let seq_no = self.engine.seq_no.fetch_add(1, Ordering::SeqCst);

        // 写数据到数据文件
        let mut positons = HashMap::new();
        for (key, item) in pending_writes.iter() {
            let record = LogRecord {
                key: log_record_key_with_seq(key.clone(), seq_no),
                value: item.value.clone(),
                rec_type: item.rec_type,
            };
            let pos = self.engine.append_log_record(record)?;
            positons.insert(item.key.clone(), pos);
        }

        // 写入标识事务完成的数据
        let fin_record = LogRecord {
            key: log_record_key_with_seq(TXN_FIN_KEY.to_vec(), seq_no),
            value: Default::default(),
            rec_type: LogRecordType::TXNFINISHED,
        };
        self.engine.append_log_record(fin_record)?;

        if self.options.sync_writes {
            self.engine.sync()?;
        }

        // 所有数据写入成功后更新索引
        for (key, record) in pending_writes.iter() {
            let pos = positons.get(key).unwrap();
            self.engine.update_index(key.clone(), record.rec_type, *pos);
        }

        // 清空暂存数据
        pending_writes.clear();

        Ok(())
    }
}

// 编码序列号和 key
pub(crate) fn log_record_key_with_seq(key: Vec<u8>, seq_no: usize) -> Vec<u8> {
    let mut enc_key = BytesMut::new();
    encode_length_delimiter(seq_no, &mut enc_key).unwrap();
    enc_key.extend_from_slice(&key);

    enc_key.to_vec()
}

pub(crate) fn parse_log_record_key(key: Vec<u8>) -> (Vec<u8>, usize) {
    let mut buf = BytesMut::new();
    buf.put_slice(&key);
    let seq_no = decode_length_delimiter(&mut buf).unwrap();

    (buf.to_vec(), seq_no)
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::atomic::Ordering};

    use crate::{options::Options, util};

    use super::*;

    #[test]
    fn test_write_batch_1() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-1");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine.new_write_batch(WriteBatchOptions::default()).unwrap();
        // 写数据之后未提交
        let put_res1 = wb.put(
            util::rand_kv::get_test_key(1),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(
            util::rand_kv::get_test_key(2),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res2.is_ok());

        let res1 = engine.get(util::rand_kv::get_test_key(1));
        assert_eq!(Errors::KeyIsNotFound, res1.err().unwrap());

        // 事务提交之后进行查询
        let commit_res = wb.commit();
        assert!(commit_res.is_ok());

        let res2 = engine.get(util::rand_kv::get_test_key(1));
        assert!(res2.is_ok());

        // 验证事务序列号
        let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
        assert_eq!(2, seq_no);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_write_batch_2() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-2");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine.new_write_batch(WriteBatchOptions::default()).unwrap();
        let put_res1 = wb.put(
            util::rand_kv::get_test_key(1),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(
            util::rand_kv::get_test_key(2),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res2.is_ok());
        let commit_res1 = wb.commit();
        assert!(commit_res1.is_ok());

        let put_res3 = wb.put(
            util::rand_kv::get_test_key(1),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res3.is_ok());

        let commit_res2 = wb.commit();
        assert!(commit_res2.is_ok());

        // 重启之后进行校验
        engine.close().expect("failed to close");
        std::mem::drop(engine);

        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys();
        assert_eq!(2, keys.len());

        // 验证事务序列号
        let seq_no = engine2.seq_no.load(Ordering::SeqCst);
        assert_eq!(3, seq_no);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    // #[test]
    // fn test_write_batch_3() {
    //     let mut opts = Options::default();
    //     opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-3");
    //     opts.data_file_size = 64 * 1024 * 1024;
    //     let engine = Engine::open(opts.clone()).expect("failed to open engine");

    //     let keys = engine.list_keys();
    //     println!("key len {:?}", keys.len());

    //     // let mut wb_opts = WriteBatchOptions::default();
    //     // wb_opts.max_batch_num = 10000000;
    //     // let wb = engine.new_write_batch(wb_opts);

    //     // for i in 0..=1000000 {
    //     //     let put_res = wb.put(util::rand_kv::get_test_key(i), util::rand_kv::get_test_value(10));
    //     //     assert!(put_res.is_ok());
    //     // }

    //     // wb.commit();
    // }
}
