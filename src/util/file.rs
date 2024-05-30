use std::path::PathBuf;

/// 磁盘数据目录的大小
pub fn dir_disk_size(dir_path: PathBuf) -> u64 {
    if let Ok(size) = fs_extra::dir::get_size(dir_path) {
        return size;
    }
    0
}

/// 获取空闲磁盘空间大小
pub fn available_disk_size() -> u64 {
    if let Ok(size) = fs2::available_space(PathBuf::from("/")) {
        return size;
    }
    0 
}