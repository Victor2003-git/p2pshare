use serde::{Deserialize, Serialize};

// ─── Protocole UDP (annonces multicast) ───────────────────────────────────────

/// Paquet UDP envoyé périodiquement par chaque pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnouncePacket {
    pub name: String,
    pub ip: String,
    pub tcp_port: u16,
    pub files: Vec<FileInfo>,
}

/// Métadonnées d'un fichier partagé
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub size: u64,
    pub sha256: String,
}

// ─── Protocole TCP (serveur de transfert) ────────────────────────────────────

/// Commandes envoyées par le client au serveur
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "UPPERCASE")]
pub enum TcpRequest {
    /// Lister les fichiers disponibles
    List,
    /// Télécharger un fichier (avec offset optionnel pour reprise)
    Get { filename: String, offset: u64 },
}

/// Réponses du serveur
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TcpResponse {
    /// Liste des fichiers
    FileList { files: Vec<FileInfo> },
    /// Métadonnées avant envoi du fichier
    FileStart { name: String, size: u64, sha256: String, offset: u64 },
    /// Fichier introuvable
    NotFound { filename: String },
    /// Erreur générique
    Error { message: String },
}

// ─── Multi-sources ────────────────────────────────────────────────────────────

/// Description d'un chunk à télécharger
#[derive(Debug, Clone)]
pub struct ChunkTask {
    pub peer_ip: String,
    pub peer_port: u16,
    pub filename: String,
    pub offset: u64,
    pub length: u64,
    pub index: usize,
}
