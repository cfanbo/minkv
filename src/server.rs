use super::config;
use super::grpc_server::StoreImpl;
use crate::db_store;
use crate::grpc_server::grpc_minkv::store_server::StoreServer;
use crate::util;
use log::*;
use redis_protocol::resp2::{
    decode::decode,
    encode::encode,
    types::{OwnedFrame, Resp2Frame},
};
use std::net::SocketAddr;
// use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::{
    // io::{Read, Write},
    sync::RwLock,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::signal;
use tokio::sync::Notify;
use tokio::task::JoinSet;

pub struct Server {
    config: Arc<config::Config>,
    store: Arc<RwLock<dyn db_store::Op>>,
}

impl Server {
    pub fn new(config: Arc<config::Config>, store: Arc<RwLock<dyn db_store::Op>>) -> Server {
        Server { config, store }
    }

    async fn handle_client_connection(&self, mut stream: TcpStream) {
        let mut buffer = [0u8; 4096];

        loop {
            // 读取客户端发送的数据
            let n = match stream.read(&mut buffer).await {
                Ok(size) => size,
                Err(e) => {
                    error!("Failed to read from stream: {}", e);
                    return;
                }
            };

            if n == 0 {
                break; // 连接已关闭
            }

            // 解析 RESP 帧
            match decode(&buffer[..n]) {
                Ok(Some((frame, _))) => match self.handle_frame(&frame) {
                    Ok(resp) => {
                        let mut buf = vec![0; resp.encode_len()];
                        encode(&mut buf, &resp).unwrap();
                        stream.write_all(&buf).await.unwrap();
                    }
                    Err(e) => {
                        let error_message = format!("-ERR {}\r\n", e);
                        stream.write_all(error_message.as_bytes()).await.unwrap();
                    }
                },
                Ok(None) => {
                    // 数据不完整，需要更多数据
                    error!("数据不完整");
                    continue;
                }
                Err(e) => {
                    error!("Error decoding frame: {}", e);
                    return;
                }
            }
        }
    }

    fn get_command_name(&self, frame: &OwnedFrame) -> Option<String> {
        if let OwnedFrame::Array(arr) = frame {
            if let Some(OwnedFrame::BulkString(bulk)) = arr.first() {
                if let Ok(command) = std::str::from_utf8(bulk) {
                    return Some(command.to_string());
                }
            }
        }
        None
    }

    fn handle_frame(&self, frame: &OwnedFrame) -> Result<OwnedFrame, String> {
        let command = match self.get_command_name(frame) {
            Some(cmd) => cmd,
            _ => return Err("(error) ERR unknown command".to_string()),
        };

        debug!("Received command: {}", command);

        match command.to_uppercase().as_str() {
            "SET" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 3 {
                        return Err("(error) ERR syntax error".to_string());
                    }
                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid SET command format".to_string()),
                    };
                    let value = match &arr[2] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid SET command format".to_string()),
                    };

                    let mut store = self.store.write().unwrap();
                    store.set(key, value, 0);

                    Ok(OwnedFrame::SimpleString(b"OK".to_vec()))
                } else {
                    Err("Invalid SET command format".to_string())
                }
            }
            "GET" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 2 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'get' command".to_string()
                        );
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid GET command format".to_string()),
                    };

                    let store = self.store.read().unwrap();
                    match store.get(key) {
                        Ok(val) => Ok(OwnedFrame::BulkString(val)),
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Null)
                        }
                    }
                } else {
                    Err("Invalid GET command format".to_string())
                }
            }
            "DEL" => {
                if let OwnedFrame::Array(arr) = frame {
                    // 检查参数个数，SET 命令应该有三个参数: SET, key, value
                    if arr.len() != 2 {
                        return Err("Invalid SET command format: expected 2 arguments".to_string());
                    }
                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid SET command format".to_string()),
                    };

                    let mut store = self.store.write().unwrap();
                    store.delete(key);
                    // store.set(key.clone(), value.clone());
                    Ok(OwnedFrame::SimpleString(b"OK".to_vec()))
                } else {
                    Err("Invalid SET command format".to_string())
                }
            }
            "EXISTS" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() < 2 {
                        return Err("(error) ERR wrong number of arguments for 'exists' command"
                            .to_string());
                    }

                    let mut count: i64 = 0;

                    let store = self.store.read().unwrap();
                    for key in &arr[1..] {
                        if let OwnedFrame::BulkString(bulk) = key {
                            // 处理 BulkString 变体
                            if store.get(bulk).is_ok() {
                                count += 1
                            }
                        }
                    }

                    Ok(OwnedFrame::Integer(count))
                } else {
                    Err("Invalid EXISTS command format".to_string())
                }
            }
            "GETSET" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 3 {
                        return Err("(error) ERR wrong number of arguments for 'getset' command"
                            .to_string());
                    }
                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid SET command format".to_string()),
                    };
                    let value = match &arr[2] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid GETSET command format".to_string()),
                    };

                    let mut store = self.store.write().unwrap();
                    let old_value = match store.get(key) {
                        Ok(val) => Ok(OwnedFrame::BulkString(val)),
                        Err(_) => Ok(OwnedFrame::Null),
                    };
                    store.set(key, value, 0);

                    old_value
                } else {
                    Err("Invalid GETSET command format".to_string())
                }
            }
            "MSET" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() < 2 || arr.len() % 2 != 1 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'mset' command".to_string()
                        );
                    }
                    // OwnedFrame::BulkString(val)
                    let mut store = self.store.write().unwrap();
                    for i in 0..arr.len() / 2 {
                        let key = match &arr[i * 2 + 1] {
                            OwnedFrame::BulkString(bulk) => bulk,
                            _ => return Err("Invalid SET command format".to_string()),
                        };

                        let value = match &arr[i * 2 + 2] {
                            OwnedFrame::BulkString(bulk) => bulk,
                            _ => return Err("Invalid SET command format".to_string()),
                        };

                        // set
                        store.set(key, value, 0);
                    }

                    Ok(OwnedFrame::SimpleString(b"OK".to_vec()))
                } else {
                    Err("Invalid MSET command format".to_string())
                }
            }
            "MGET" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() < 2 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'mget' command".to_string()
                        );
                    }
                    // OwnedFrame::BulkString(val)
                    let mut result = Vec::new();

                    let store = self.store.read().unwrap();
                    for key in &arr[1..] {
                        if let OwnedFrame::BulkString(bulk) = key {
                            match store.get(bulk) {
                                Ok(value) => result.push(OwnedFrame::BulkString(value)),
                                Err(_) => result.push(OwnedFrame::Null),
                            };
                        }
                    }

                    Ok(OwnedFrame::Array(result))
                } else {
                    Err("Invalid MGET command format".to_string())
                }
            }
            "APPEND" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 3 {
                        return Err("(error) ERR syntax error".to_string());
                    }
                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid APPEND command format".to_string()),
                    };
                    let value = match &arr[2] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid APPEND command format".to_string()),
                    };

                    let mut store = self.store.write().unwrap();
                    match store.get(key) {
                        Ok(mut val) => {
                            val.extend(value);
                            store.set(key, &val, 0);
                            Ok(OwnedFrame::Integer(val.len() as i64))
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            store.set(key, value, 0);
                            Ok(OwnedFrame::Integer(value.len() as i64))
                        }
                    }
                } else {
                    Err("Invalid APPEND command format".to_string())
                }
            }
            "INCR" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 2 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'incr' command".to_string()
                        );
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid INCR command format".to_string()),
                    };

                    let mut store = self.store.write().unwrap();
                    match store.get(key) {
                        Ok(val) => {
                            // convert to a number
                            let s = String::from_utf8_lossy(&val);
                            match s.parse::<i64>() {
                                Ok(mut n) => {
                                    n += 1;
                                    store.set(key, &n.to_string().into_bytes(), 0);
                                    Ok(OwnedFrame::Integer(n))
                                }
                                Err(_) => Ok(OwnedFrame::Error(
                                    "(error) ERR value is not an integer or out of range"
                                        .to_string(),
                                )),
                            }
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Null)
                        }
                    }
                } else {
                    Err("Invalid INCR command format".to_string())
                }
            }
            "DECR" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 2 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'incr' command".to_string()
                        );
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid DECR command format".to_string()),
                    };

                    let mut store = self.store.write().unwrap();
                    match store.get(key) {
                        Ok(val) => {
                            // convert to a number
                            let s = String::from_utf8_lossy(&val);
                            match s.parse::<i64>() {
                                Ok(mut n) => {
                                    n -= 1;
                                    store.set(key, &n.to_string().into_bytes(), 0);
                                    Ok(OwnedFrame::Integer(n))
                                }
                                Err(_) => Ok(OwnedFrame::Error(
                                    "(error) ERR value is not an integer or out of range"
                                        .to_string(),
                                )),
                            }
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Null)
                        }
                    }
                } else {
                    Err("Invalid DECR command format".to_string())
                }
            }
            "INCRBY" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 3 {
                        return Err("(error) ERR wrong number of arguments for 'incrby' command"
                            .to_string());
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid INCRBY command format".to_string()),
                    };

                    let num_str = match &arr[2] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid INCRBY command format".to_string()),
                    };

                    let s = String::from_utf8_lossy(num_str);
                    let value = match s.parse::<i64>() {
                        Ok(value) => value,
                        Err(_) => {
                            return Ok(OwnedFrame::Error(
                                "(error) ERR value is not an integer or out of range".to_string(),
                            ))
                        }
                    };

                    let mut store = self.store.write().unwrap();
                    match store.get(key) {
                        Ok(val) => {
                            // convert to a number
                            let s = String::from_utf8_lossy(&val);
                            match s.parse::<i64>() {
                                Ok(mut n) => {
                                    n += value;
                                    store.set(key, &n.to_string().into_bytes(), 0);
                                    Ok(OwnedFrame::Integer(n))
                                }
                                Err(_) => Ok(OwnedFrame::Error(
                                    "(error) ERR value is not an integer or out of range"
                                        .to_string(),
                                )),
                            }
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Null)
                        }
                    }
                } else {
                    Err("Invalid INCRBY command format".to_string())
                }
            }
            "DECRBY" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 3 {
                        return Err("(error) ERR wrong number of arguments for 'decrby' command"
                            .to_string());
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid DECRBY command format".to_string()),
                    };

                    let num_str = match &arr[2] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid DECRBY command format".to_string()),
                    };

                    let s = String::from_utf8_lossy(num_str);
                    let value = match s.parse::<i64>() {
                        Ok(value) => value,
                        Err(_) => {
                            return Ok(OwnedFrame::Error(
                                "(error) ERR value is not an integer or out of range".to_string(),
                            ))
                        }
                    };

                    let mut store = self.store.write().unwrap();
                    match store.get(key) {
                        Ok(val) => {
                            // convert to a number
                            let s = String::from_utf8_lossy(&val);
                            match s.parse::<i64>() {
                                Ok(mut n) => {
                                    n -= value;
                                    store.set(key, &n.to_string().into_bytes(), 0);
                                    Ok(OwnedFrame::Integer(n))
                                }
                                Err(_) => Ok(OwnedFrame::Error(
                                    "(error) ERR value is not an integer or out of range"
                                        .to_string(),
                                )),
                            }
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Null)
                        }
                    }
                } else {
                    Err("Invalid DECRBY command format".to_string())
                }
            }
            "EXPIRE" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 3 {
                        return Err("(error) ERR wrong number of arguments for 'expire' command"
                            .to_string());
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid EXPIRE command format".to_string()),
                    };

                    let num_str = match &arr[2] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid EXPIRE command format".to_string()),
                    };

                    let s = String::from_utf8_lossy(num_str);
                    let value = match s.parse::<u64>() {
                        Ok(value) => value,
                        Err(_) => {
                            return Ok(OwnedFrame::Error(
                                "(error) ERR value is not an integer or out of range".to_string(),
                            ))
                        }
                    };
                    if value > u64::MAX / 1000 {
                        return Ok(OwnedFrame::Error(
                            "(error) ERR invalid expire time in 'expire' command".to_string(),
                        ));
                    }

                    let millisec = util::time::get_millisec_from_sec(value);
                    let mut store = self.store.write().unwrap();
                    match store.get(key) {
                        Ok(val) => {
                            store.set(key, &val, millisec);
                            Ok(OwnedFrame::Integer(1))
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Null)
                        }
                    }
                } else {
                    Err("Invalid EXPIRE command format".to_string())
                }
            }
            "EXPIREAT" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 3 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'expireat' command"
                                .to_string(),
                        );
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid EXPIREAT command format".to_string()),
                    };

                    let num_str = match &arr[2] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid EXPIREAT command format".to_string()),
                    };

                    let s = String::from_utf8_lossy(num_str);
                    let value = match s.parse::<u64>() {
                        Ok(value) => value,
                        Err(_) => {
                            return Ok(OwnedFrame::Error(
                                "(error) ERR value is not an integer or out of range".to_string(),
                            ))
                        }
                    };
                    if value > u64::MAX / 1000 {
                        return Ok(OwnedFrame::Error(
                            "(error) ERR invalid expire time in 'expireat' command".to_string(),
                        ));
                    }

                    let millisec = util::time::sec_to_millisec(value);
                    let mut store = self.store.write().unwrap();
                    match store.get(key) {
                        Ok(val) => {
                            store.set(key, &val, millisec);
                            Ok(OwnedFrame::Integer(1))
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Null)
                        }
                    }
                } else {
                    Err("Invalid EXPIREAT command format".to_string())
                }
            }
            "PEXPIRE" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 3 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'pexpire' command"
                                .to_string(),
                        );
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid PEXPIRE command format".to_string()),
                    };

                    let num_str = match &arr[2] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid PEXPIRE command format".to_string()),
                    };

                    let s = String::from_utf8_lossy(num_str);
                    let value = match s.parse::<u64>() {
                        Ok(value) => value,
                        Err(_) => {
                            return Ok(OwnedFrame::Error(
                                "(error) ERR value is not an integer or out of range".to_string(),
                            ))
                        }
                    };
                    if util::time::current_milliseconds() + value > u64::MAX / 1000 {
                        return Ok(OwnedFrame::Error(
                            "(error) ERR invalid expire time in 'pexpire' command".to_string(),
                        ));
                    }

                    let millisec = util::time::get_millisec(value);
                    let mut store = self.store.write().unwrap();
                    match store.get(key) {
                        Ok(val) => {
                            store.set(key, &val, millisec);
                            Ok(OwnedFrame::Integer(1))
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Null)
                        }
                    }
                } else {
                    Err("Invalid PEXPIRE command format".to_string())
                }
            }
            "PEXPIREAT" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 3 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'pexpireat' command"
                                .to_string(),
                        );
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid PEXPIREAT command format".to_string()),
                    };

                    let num_str = match &arr[2] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid PEXPIREAT command format".to_string()),
                    };

                    let s = String::from_utf8_lossy(num_str);
                    let value = match s.parse::<u64>() {
                        Ok(value) => value,
                        Err(_) => {
                            return Ok(OwnedFrame::Error(
                                "(error) ERR value is not an integer or out of range".to_string(),
                            ))
                        }
                    };
                    if value > u64::MAX / 1000 {
                        return Ok(OwnedFrame::Error(
                            "(error) ERR invalid expire time in 'pexpireat' command".to_string(),
                        ));
                    }

                    let mut store = self.store.write().unwrap();
                    match store.get(key) {
                        Ok(val) => {
                            store.set(key, &val, value);
                            Ok(OwnedFrame::Integer(1))
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Null)
                        }
                    }
                } else {
                    Err("Invalid PEXPIREAT command format".to_string())
                }
            }
            "TTL" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 2 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'ttl' command".to_string()
                        );
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid TTL command format".to_string()),
                    };

                    let store = self.store.write().unwrap();
                    match store.get_entry(key) {
                        Ok(entry) => {
                            if entry.timestamp == 0 {
                                // 未设置过期时间
                                Ok(OwnedFrame::Integer(-1))
                            } else if entry.is_expired() {
                                Ok(OwnedFrame::Null)
                            } else {
                                Ok(OwnedFrame::Integer(
                                    util::time::get_lifetime_sec(entry.timestamp) as i64,
                                ))
                            }
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Integer(-2))
                        }
                    }
                } else {
                    Err("Invalid TTL command format".to_string())
                }
            }
            "PTTL" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 2 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'pttl' command".to_string()
                        );
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid PTTL command format".to_string()),
                    };

                    let store = self.store.write().unwrap();
                    match store.get_entry(key) {
                        Ok(entry) => {
                            if entry.timestamp == 0 {
                                // 未设置过期时间
                                Ok(OwnedFrame::Integer(-1))
                            } else if entry.is_expired() {
                                Ok(OwnedFrame::Null)
                            } else {
                                Ok(OwnedFrame::Integer(util::time::get_lifetime_millisec(
                                    entry.timestamp,
                                )
                                    as i64))
                            }
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Integer(-2))
                        }
                    }
                } else {
                    Err("Invalid PTTL command format".to_string())
                }
            }
            "PERSIST" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 2 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'persist' command"
                                .to_string(),
                        );
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid PERSIST command format".to_string()),
                    };

                    let mut store = self.store.write().unwrap();
                    match store.get_entry(key) {
                        Ok(entry) => {
                            if entry.timestamp == 0 {
                                // 未设置过期时间
                                Ok(OwnedFrame::Integer(0))
                            } else {
                                store.set(key, &entry.value, 0);
                                Ok(OwnedFrame::Integer(1))
                            }
                        }
                        Err(e) => {
                            debug!("get value occur error {:?}", e);
                            Ok(OwnedFrame::Integer(0))
                        }
                    }
                } else {
                    Err("Invalid PERSIST command format".to_string())
                }
            }
            "KEYS" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() != 2 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'keys' command".to_string()
                        );
                    }

                    let key = match &arr[1] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid KEYS command format".to_string()),
                    };

                    let k = String::from_utf8(key.to_vec()).unwrap();
                    let store = self.store.read().unwrap();
                    let mut result: Vec<OwnedFrame> = Vec::new();
                    for v in store.keys() {
                        let v1 = String::from_utf8(v.to_vec()).unwrap();
                        if util::match_key(&k, &v1) {
                            if let Ok(entry) = store.get_entry(v.as_slice()) {
                                if !entry.is_expired() {
                                    result.push(OwnedFrame::BulkString(v.clone()));
                                }
                            }
                        }
                    }
                    Ok(OwnedFrame::Array(result))
                } else {
                    Err("Invalid KEYS command format".to_string())
                }
            }
            "PING" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() == 1 {
                        return Ok(OwnedFrame::SimpleString(b"PONG".to_vec()));
                    }
                    if arr.len() != 2 {
                        return Err(
                            "(error) ERR wrong number of arguments for 'ping' command".to_string()
                        );
                    }
                    let key = match &arr[2] {
                        OwnedFrame::BulkString(bulk) => bulk,
                        _ => return Err("Invalid PING command format".to_string()),
                    };

                    Ok(OwnedFrame::BulkString(key.to_vec()))
                } else {
                    Err("Invalid PING command format".to_string())
                }
            }
            "CLIENT" => {
                if let OwnedFrame::Array(arr) = frame {
                    if arr.len() < 2 {
                        return Err("(error) ERR wrong number of arguments for 'CLIENT' command"
                            .to_string());
                    }

                    Ok(OwnedFrame::SimpleString(b"OK".to_vec()))
                } else {
                    Err("Invalid CLIENT command format".to_string())
                }
            }
            // 其他命令处理
            _ => Err(format!("Unsupported command {:?}", command.as_str()).to_string()),
        }
    }

    pub async fn server_start(self: Arc<Self>, notify: Arc<Notify>) -> anyhow::Result<()> {
        let addr = self.config.get_addr()?;
        let listener = TcpListener::bind(addr).await?;
        println!("Listening on {}", addr);

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _)) => {
                            debug!("New connection: {}", stream.peer_addr().unwrap());
                            let server_clone = Arc::clone(&self);
                            tokio::spawn(async move {
                                server_clone.handle_client_connection(stream).await;
                            });
                        },
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                },
                _ = notify.notified() => {
                    println!("Shutdown signal received. Stopping server...");
                    break;
                }
            }
        }
        Ok(())
    }
}

pub async fn start_server(option: &Option<PathBuf>) -> anyhow::Result<()> {
    // 创建一个 Notify 对象，用于通知所有任务停止
    let notify = Arc::new(Notify::new());

    let conf = if let Some(file) = option {
        config::Config::try_from(file.as_path())?
    } else {
        config::Config::new()?
    };
    debug!("{:?}", conf);

    // common core data
    let the_config = Arc::new(conf);

    let (tx, rx) = mpsc::channel();
    let store = db_store::new_store(Arc::clone(&the_config), tx, rx);
    let store = Arc::new(RwLock::new(store));

    // joinset
    let mut join_set = JoinSet::new();

    // 使用 tokio::task::spawn_blocking 启动同步的 TCP 服务器
    let store_clone = Arc::clone(&store);
    let srv = Server::new(Arc::clone(&the_config), store_clone);
    let server = Arc::new(srv);
    // let tcp_handle = task::spawn_blocking(|| {
    //     server.server_start();
    // });

    let notify_clone = Arc::clone(&notify);
    join_set.spawn(async move {
        // 使用 block_in_place 处理阻塞操作
        server.server_start(notify_clone).await.unwrap();
    });

    // 添加 gRPC 服务器任务（如果配置存在）
    if let Some(grpc_config) = the_config.get_grpc() {
        let store_clone = Arc::clone(&store);
        let addr = grpc_config.get_addr()?;
        let notify_clone = Arc::clone(&notify);
        join_set.spawn(async move {
            run_grpc_server(addr, store_clone, notify_clone)
                .await
                .unwrap();
        });
    }

    // 监听 Ctrl+C 信号
    signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
    info!("Received Ctrl+C, shutting down...");

    // 通知所有任务停止
    notify.notify_waiters();

    // 等待所有任务完成
    while let Some(Ok(_)) = join_set.join_next().await {}

    Ok(())
}

// gRPC server
async fn run_grpc_server(
    addr: SocketAddr,
    store: Arc<RwLock<dyn db_store::Op>>,
    notify: Arc<Notify>,
) -> anyhow::Result<()> {
    println!("gRPC Server Listening on {:?}", addr);

    tonic::transport::Server::builder()
        .add_service(StoreServer::new(StoreImpl::new(store)))
        .serve_with_shutdown(addr, async {
            notify.notified().await;
        })
        // .serve(addr)
        .await?;

    Ok(())
}
