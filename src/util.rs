use log::*;
use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};
pub mod lock;
pub mod time;

pub fn get_files_in_directory(path: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    // 检查目录是否存在
    if !path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Directory does not exist",
        ));
    }

    // 检查路径是否是目录
    if !path.is_dir() {
        debug!("文件 {:?} 是否为一个路径", path);
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Path is not a directory",
        ));
    }

    // 读取目录下的所有条目
    let mut file_list = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();

        // 只添加文件，不包括子目录
        if path.is_file() {
            file_list.push(path);
        }
    }

    file_list.sort();

    Ok(file_list)
}

pub fn filter_file(file_path: PathBuf, prefix: &str, ext: &str) -> bool {
    // 检查文件名前缀
    if let Some(file_name) = file_path.file_stem() {
        if let Some(file_name_str) = file_name.to_str() {
            if !file_name_str.starts_with(prefix) {
                debug!("忽略文件 {:?}", file_name_str);
                return false;
            }
        }
    }

    // 检查文件扩展名
    if let Some(file_ext) = file_path.extension() {
        debug!("检查datafile 文件扩展名 {:?} = {:?}", file_ext, ext);
        if file_ext != ext {
            return false;
        }
    } else {
        return false;
    }

    true
}

pub fn parse_seq_from_filename(filename: PathBuf) -> usize {
    let main_filename = filename.file_stem().unwrap().to_string_lossy();

    // 提取数字部分
    let number_str = main_filename.split('-').last().unwrap(); // 取最后一个部分
    let number: u32 = number_str.parse().unwrap();

    println!("main_filename: {:?}", main_filename);
    number as usize
}

// Helper function to format bytes as a UTF-8 string
pub fn format_bytes_as_str(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => "[invalid UTF-8]".to_string(),
    }
}

// 模仿 Redis 的 KEYS 命令,模式支持 '*' 和 '?', 实现常用正则
pub fn match_key(search_str: &str, key: &str) -> bool {
    // 将搜索字符串转换为正则表达式
    let regex_pattern = search_str.replace("*", ".*").replace("?", ".");
    match Regex::new(&format!("^{}$", regex_pattern)) {
        Ok(regex) => regex.is_match(key),
        Err(_) => false,
    }
}
