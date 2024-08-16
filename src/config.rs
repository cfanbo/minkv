use regex::Regex;
use serde::Deserialize;
use std::io::{Error, ErrorKind};
use std::net::IpAddr;
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Deserialize)]
struct FileConfig {
    db_dir: Option<String>,
    file: Option<String>,
    file_max_size: Option<u32>,
    sync_keys: Option<u32>,
    merge_file_num: Option<u32>,
    server: Option<FileConfigServer>,
}

#[derive(Debug, Deserialize)]
struct FileConfigServer {
    address: Option<String>,
    port: Option<u32>,
}

impl TryFrom<&Path> for FileConfig {
    type Error = anyhow::Error;

    fn try_from(path: &Path) -> Result<FileConfig, Self::Error> {
        if !path.exists() {
            return Err(Error::new(ErrorKind::NotFound, "config file not exists!").into());
        }

        let body = std::fs::read_to_string(path)?;
        let config: FileConfig = toml::from_str(&body)?;

        if let Some(server) = &config.server {
            if let Some(address) = &server.address {
                if !address.parse::<IpAddr>().is_ok() {
                    return Err(Error::new(
                        ErrorKind::AddrNotAvailable,
                        "server address invalid".to_string(),
                    )
                    .into());
                }
            }

            if let Some(port) = server.port {
                if port < 1 || port > 65535 {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("invalid port {}", port).to_string(),
                    )
                    .into());
                }
            }
        }

        Ok(config)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    db_dir: String,
    file: String,
    file_max_size: usize, // 字节
    sync_keys: u32,
    server: ConfigServer,
    merge_file_num: usize,
}
#[derive(Debug, Deserialize, Clone)]
struct ConfigServer {
    address: String,
    port: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            db_dir: "./dbdata".to_string(),
            file: String::from("data"),
            file_max_size: 1024 * 100,
            sync_keys: 0,
            server: ConfigServer {
                address: "127.0.0.1".to_string(),
                port: 6380,
            },
            merge_file_num: 10,
        }
    }
}

impl TryFrom<&Path> for Config {
    type Error = anyhow::Error;
    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        let config: FileConfig = FileConfig::try_from(path)?;

        // fill in with default values
        let mut default_config = Config::default();
        if let Some(value) = config.db_dir {
            default_config.db_dir = value;
        }

        if let Some(value) = config.file {
            default_config.file = value;
        }

        if let Some(value) = config.file_max_size {
            default_config.file_max_size = value as usize;
        }
        if let Some(value) = config.merge_file_num {
            default_config.merge_file_num = value as usize;
        }

        if let Some(value) = config.sync_keys {
            default_config.sync_keys = value
        }

        if let Some(server) = config.server {
            if let Some(server_address) = server.address {
                default_config.server.address = server_address
            }
            if let Some(server_port) = server.port {
                default_config.server.port = server_port
            }
        }

        default_config.check()?;

        Ok(default_config)
    }
}

const MERGE_DIR: &str = ".merge";
const HINT: &str = "hint";

impl Config {
    pub fn new() -> anyhow::Result<Config> {
        let config = Config::default();
        config.check()?;
        Ok(config)
    }

    fn check(&self) -> anyhow::Result<()> {
        // check if the datadir exists
        {
            let path = self.data_dir();
            if !path.exists() {
                fs::create_dir_all(path).expect(&format!("Failed to create directory: {:?}", path));
            }
        }

        // check if the merge dir exists，测试是否可写
        {
            let path = self.merge_dir();
            if !path.exists() {
                fs::create_dir_all(&path)
                    .expect(&format!("Failed to create directory: {:?}", path));
            }
            fs::remove_dir_all(path).unwrap();
        }

        Ok(())
    }

    pub fn get_addr_port(&self) -> (String, u32) {
        (self.server.address.clone(), self.server.port)
    }

    pub fn merge_cleanup(&self) {
        let dir = self.merge_dir();
        let path = Path::new(&dir);
        fs::remove_dir(path).unwrap();
    }

    pub fn file(&self) -> &str {
        &self.file
    }

    pub fn data_dir(&self) -> &Path {
        Path::new(&self.db_dir)
    }

    pub fn merge_dir(&self) -> PathBuf {
        self.data_dir().join(MERGE_DIR)
    }

    pub fn get_merge_filepath_by_seq(&self, idx: u16) -> PathBuf {
        self.merge_dir().join(idx.to_string())
    }

    pub fn get_merge_hint_filepath_by_seq(&self, idx: u16) -> PathBuf {
        let mut path = self.merge_dir().join(idx.to_string());
        path.set_extension(HINT);
        path
    }

    pub fn get_hint_filepath_by_seq(&self, idx: u16) -> PathBuf {
        let filename = format!("{}.{}", HINT, idx);
        let file = self.data_dir().join(filename);
        file
    }

    pub fn file_max_size(&self) -> usize {
        self.file_max_size
    }

    pub fn get_filepath_by_seq(&self, idx: u16) -> PathBuf {
        let filename = format!("{}.{}", self.file, idx);
        let file = self.data_dir().join(filename);
        file
    }

    pub fn get_active_filepath(&self) -> PathBuf {
        self.data_dir().join(self.file())
    }

    pub fn get_next_datafile_seq(&self) -> u16 {
        let dir_path = self.data_dir();
        let pattern = format!(r"{}\.(\d+)", self.file); // 动态生成正则表达式

        // 创建正则表达式
        let re = Regex::new(&pattern).unwrap();

        // 读取目录中的所有文件
        let mut max_number = 0;
        for entry in fs::read_dir(dir_path).unwrap() {
            let entry = entry.unwrap();
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // 尝试匹配文件名
            if let Some(caps) = re.captures(&file_name_str) {
                if let Some(number_str) = caps.get(1) {
                    if let Ok(number) = number_str.as_str().parse::<u32>() {
                        // 更新最大编号
                        if number > max_number {
                            max_number = number;
                        }
                    }
                }
            }
        }

        // seq increment
        max_number as u16 + 1
    }

    pub fn get_next_datafile(&self) -> PathBuf {
        let seq = self.get_next_datafile_seq();
        self.get_filepath_by_seq(seq)
    }

    pub fn get_merge_file_num(&self) -> usize {
        self.merge_file_num
    }

    pub fn get_sync_keys_num(&self) -> u32 {
        self.sync_keys
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, path::PathBuf};
    use tempfile::NamedTempFile;

    // #[test]
    // fn new() {
    //     let cnf = super::Config::new();
    //     let dir = cnf.merge_dir();
    //     let tpath = "./dbdata/.merge";
    //     assert_eq!(PathBuf::from(tpath), dir);
    // }

    // #[test]
    // fn get_filepath_by_seq() {
    //     let idx = 2;

    //     let cnf = super::Config::new();
    //     let fpath = cnf.get_filepath_by_seq(idx);
    //     println!("{:?}", fpath);
    //     assert_eq!(PathBuf::from("./dbdata/data.2"), fpath);
    // }

    #[test]
    fn config_new_from_file() -> anyhow::Result<()> {
        let mut tmpfile = NamedTempFile::new()?;
        let config_content = r#"
                    db_dir = "mysql_data"
                    [server]
                    port = 7788
                "#;
        writeln!(tmpfile, "{}", config_content)?;

        let fpath = PathBuf::from(tmpfile.path().to_str().unwrap());
        let config = super::Config::try_from(fpath.as_path())?;
        assert_eq!(config.db_dir, "mysql_data");
        assert_eq!(config.file, "data");
        assert_eq!(config.file_max_size, 1024 * 100);
        assert_eq!(config.server.address, String::from("127.0.0.1"));
        assert_eq!(config.server.port, 7788);
        Ok(())
    }
}
