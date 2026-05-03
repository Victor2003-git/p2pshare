use crate::protocol::{AnnouncePacket, FileInfo};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Pair actif découvert via UDP
#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub packet: AnnouncePacket,
    pub last_seen: DateTime<Utc>,
}

/// État global partagé (clone-able, thread-safe)
#[derive(Clone)]
pub struct SharedState {
    inner: Arc<Inner>,
}

struct Inner {
    pub share_dir: PathBuf,
    pub name: String,
    pub tcp_port: u16,
    pub peers: RwLock<HashMap<String, PeerEntry>>, // clé = "ip:port"
}

impl SharedState {
    pub fn new(share_dir: PathBuf, name: String, tcp_port: u16) -> Self {
        SharedState {
            inner: Arc::new(Inner {
                share_dir,
                name,
                tcp_port,
                peers: RwLock::new(HashMap::new()),
            }),
        }
    }

    pub fn share_dir(&self) -> &PathBuf {
        &self.inner.share_dir
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn tcp_port(&self) -> u16 {
        self.inner.tcp_port
    }

    /// Lire la table des pairs
    pub async fn get_peers(&self) -> HashMap<String, PeerEntry> {
        self.inner.peers.read().await.clone()
    }

    /// Mettre à jour ou ajouter un pair
    pub async fn upsert_peer(&self, key: String, entry: PeerEntry) {
        self.inner.peers.write().await.insert(key, entry);
    }

    /// Supprimer les pairs inactifs depuis > 30 secondes
    pub async fn evict_stale_peers(&self) {
        let threshold = chrono::Duration::seconds(30);
        let now = Utc::now();
        self.inner
            .peers
            .write()
            .await
            .retain(|_, e| now - e.last_seen < threshold);
    }

    /// Scanner le dossier partagé et retourner les FileInfo
    pub async fn list_local_files(&self) -> Vec<FileInfo> {
        use sha2::{Digest, Sha256};
        use tokio::io::AsyncReadExt;

        let mut files = Vec::new();
        let dir = self.inner.share_dir.clone();

        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(_) => return files,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            let meta = match tokio::fs::metadata(&path).await {
                Ok(m) => m,
                Err(_) => continue,
            };
            let size = meta.len();

            // Calculer SHA-256
            let sha256 = if let Ok(mut f) = tokio::fs::File::open(&path).await {
                let mut hasher = Sha256::new();
                let mut buf = vec![0u8; 65536];
                loop {
                    match f.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => hasher.update(&buf[..n]),
                        Err(_) => break,
                    }
                }
                hex::encode(hasher.finalize())
            } else {
                String::from("?")
            };

            files.push(FileInfo { name, size, sha256 });
        }

        files
    }
}
