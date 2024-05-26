use log::error;
use std::{
    fs::{create_dir_all, read_dir, remove_dir_all, remove_file, rename},
    path::PathBuf,
};

use crate::{
    batch::{log_record_key_with_seq, parse_log_record_key, NON_TXN_SEQ_NO},
    data::{
        data_file::{get_data_file_path, DataFile, DATA_FILE_NAME_SUFFIX, HINT_FILE_NAME, MERGE_FINISHED_FILE_NAME, SEQ_NO_FILE_NAME},
        log_record::{decode_log_record_pos, LogRecord, LogRecordType},
    },
    db::{Engine, FILE_LOCK_NAME},
    errors::Errors,
    options::{IOType, Options},
};

const MERGE_DIR_NAME: &str = "merge";
const MERGE_FIN_KEY: &[u8] = "merge.finished".as_bytes();

impl Engine {
    // merge 数据目录，处理无效数据，并生成 hint 索引文件
    pub fn merge(&self) -> Result<(), Errors> {
        let lock = self.merging_lock.try_lock();
        if lock.is_none() {
            return Err(Errors::MergeInProcess);
        }

        // 如果 merge 目录已经存在，删除并新建 merge 目录
        let merge_path = get_merge_path(self.options.dir_path.clone());
        if merge_path.is_dir() {
            remove_dir_all(merge_path.clone()).unwrap();
        }
        if let Err(e) = create_dir_all(merge_path.clone()) {
            error!("failed to create merge path {}", e);
            return Err(Errors::FailedCreateDatabaseDir);
        }

        let merge_files = self.get_merge_files()?;

        // 打开用于 merge 的存储引擎实例
        let mut merge_opts = Options::default();
        merge_opts.dir_path = merge_path.clone();
        merge_opts.data_file_size = self.options.data_file_size;
        let merge_engine = Engine::open(merge_opts)?;

        // 打开 hint 索引文件
        let mut hint_file = DataFile::new_hint_file(merge_path.clone())?;

        // 处理所有 merge 文件，重写有效的数据
        for data_file in merge_files.iter() {
            let mut offset = 0;
            loop {
                let (mut log_record, size) = match data_file.read(offset) {
                    Ok(read_result) => (read_result.record, read_result.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        }
                        return Err(e);
                    }
                };

                // 解码拿到实际的 key
                let (real_key, _) = parse_log_record_key(log_record.key);
                if let Some(index_pos) = self.index.get(real_key.clone()) {
                    // 索引中数据位置信息与当前数据位置信息一致，说明当前数据有效
                    if index_pos.file_id == data_file.get_file_id() && index_pos.offset == offset {
                        // 取出 key 的事务标识
                        log_record.key = log_record_key_with_seq(real_key.clone(), NON_TXN_SEQ_NO);
                        let pos = merge_engine.append_log_record(log_record)?;
                        hint_file.write_hint_record(real_key.clone(), pos)?;
                    }
                }

                offset += size;
            }
        }

        // merge 文件和 hint 文件持久化
        merge_engine.sync()?;
        hint_file.sync()?;

        // 拿到最近未参与 merge 的文件 id，将其写入到文件中标识 merge 成功
        let non_merge_file_id = merge_files.last().unwrap().get_file_id() + 1;
        let mut merge_fin_file = DataFile::new_merge_finished_file(merge_path.clone())?;
        let merge_fin_record = LogRecord {
            key: MERGE_FIN_KEY.to_vec(),
            value: non_merge_file_id.to_string().into_bytes(),
            rec_type: LogRecordType::NOAMAL,
        };
        let enc_record = merge_fin_record.encode();
        merge_fin_file.write(&&enc_record)?;
        merge_fin_file.sync()?;

        Ok(())
    }

    // 获取所有需要 merge 的数据文件
    fn get_merge_files(&self) -> Result<Vec<DataFile>, Errors> {
        let mut active_file = self.active_file.write();
        let mut older_files = self.older_files.write();

        // 持久化当前活跃文件并加入到旧文件列表，设置新的活跃文件
        active_file.sync()?;
        let current_fid = active_file.get_file_id();
        let old_file = DataFile::new(self.options.dir_path.clone(), current_fid, IOType::FileIO)?;
        older_files.insert(current_fid, old_file);
        let new_file = DataFile::new(self.options.dir_path.clone(), current_fid + 1, IOType::FileIO)?;
        *active_file = new_file;

        // merge 文件从小到大依次 merge
        let mut merge_file_ids = Vec::new();
        for fid in older_files.keys() {
            merge_file_ids.push(*fid);
        }
        merge_file_ids.sort();

        let mut merge_files = Vec::new();
        for fid in merge_file_ids.iter() {
            merge_files.push(DataFile::new(self.options.dir_path.clone(), *fid, IOType::FileIO)?);
        }

        Ok(merge_files)
    }

    // 从 hint 文件中加载索引
    pub(crate) fn load_index_from_hint_file(&self) -> Result<(), Errors> {
        let hint_file_name = self.options.dir_path.join(HINT_FILE_NAME);
        if !hint_file_name.is_file() {
            return Ok(());
        }

        let hint_file = DataFile::new_hint_file(self.options.dir_path.clone())?;
        let mut offset = 0;
        loop {
            let (log_record, size) = match hint_file.read(offset) {
                Ok(read_res) => (read_res.record, read_res.size),
                Err(e) => {
                    if e == Errors::ReadDataFileEOF {
                        break;
                    }
                    return Err(e);
                }
            };
            // 解析 value 得到 key 位置信息，添加到内存索引
            let pos = decode_log_record_pos(log_record.value);
            self.index.put(log_record.key, pos);

            offset += size; 
        }

        Ok(())
    }
}

// 获取临时用于 merge 的数据目录
fn get_merge_path(dir_path: PathBuf) -> PathBuf {
    let file_name = dir_path.file_name().unwrap();
    let merge_name = format!("{}-{}", file_name.to_str().unwrap(), MERGE_DIR_NAME);
    let parent = dir_path.parent().unwrap();
    parent.to_path_buf().join(merge_name)
}

// 加载 merge 数据目录
pub(crate) fn load_merge_files(dir_path: PathBuf) -> Result<(), Errors> {
    let merge_path = get_merge_path(dir_path.clone());
    // 没有发生过 merge 则直接返回
    if !merge_path.is_dir() {
        return Ok(());
    }

    let dir = match read_dir(merge_path.clone()) {
        Ok(dir) => dir,
        Err(e) => {
            error!("failed to read merge dir: {}", e);
            return Err(Errors::FailedOpenDatabaseDir);
        }
    };

    // 读到标识 merge 完成的文件才能继续
    let mut merge_file_names = Vec::new();
    let mut merge_finished = false;
    for file in dir {
        if let Ok(entry) = file {
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();

            if file_name.ends_with(MERGE_FINISHED_FILE_NAME) {
                merge_finished = true;
            }
            if file_name.ends_with(SEQ_NO_FILE_NAME) {
                continue;
            }
            if file_name.ends_with(FILE_LOCK_NAME) {
                continue;
            }

            // 数据文件容量为空则跳过
            let meta = entry.metadata().unwrap();
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) && meta.len() == 0 {
                continue;
            }
            merge_file_names.push(entry.file_name());
        }
    }

    // merge 没有完成，删除 merge 目录并返回
    if !merge_finished {
        remove_dir_all(merge_path.clone()).unwrap();
        return Ok(());
    }

    // 拿到最近未参与 merge 的文件 id
    let merge_fin_file = DataFile::new_merge_finished_file(merge_path.clone())?;
    let read_res = merge_fin_file.read(0)?;
    let v = String::from_utf8(read_res.record.value).unwrap();
    let non_merge_id = v.parse::<u32>().unwrap();

    // 删除旧的数据文件
    for fid in 0..non_merge_id {
        let file_path = get_data_file_path(dir_path.clone(), fid);
        remove_file(file_path).unwrap();
    }

    // 将 merge 文件移动到数据目录
    for file_name in merge_file_names {
        let src_path = merge_path.join(file_name.clone());
        let dest_path = dir_path.join(file_name.clone());
        rename(src_path, dest_path).unwrap();
    }

    // 删除 merge 目录
    remove_dir_all(merge_path).unwrap();

    Ok(())
}
