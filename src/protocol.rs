use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnouncePacket {
    pub name: String,
    pub ip: String,
    pub tcp_port: u16,
    pub files: Vec<FileInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "UPPERCASE")]
pub enum TcpRequest {
    List,
    Get { filename: String, offset: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TcpResponse {
    FileList { files: Vec<FileInfo> },
    FileStart { name: String, size: u64, sha256: String, offset: u64 },
    NotFound { filename: String },
    Error { message: String },
}

#[derive(Debug, Clone)]
pub struct ChunkTask {
    pub peer_ip: String,
    pub peer_port: u16,
    pub filename: String,
    pub offset: u64,
    pub length: u64,
    pub index: usize,
}
