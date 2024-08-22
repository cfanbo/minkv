use std::time::SystemTime;

// 获取当前时间的毫秒时间戳
pub fn current_milliseconds() -> u64 {
    let now = SystemTime::now();
    let duration_since_epoch = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    duration_since_epoch.as_millis() as u64
}

pub fn get_millisec_from_sec(sec: u64) -> u64 {
    current_milliseconds() + sec * 1000
}

pub fn get_millisec(millisec: u64) -> u64 {
    current_milliseconds() + millisec
}

pub fn sec_to_millisec(sec: u64) -> u64 {
    sec * 1000
}

pub fn get_lifetime_sec(expire_millisec: u64) -> u64 {
    (expire_millisec - current_milliseconds()) / 1000
}

pub fn get_lifetime_millisec(expire_millisec: u64) -> u64 {
    expire_millisec - current_milliseconds()
}
