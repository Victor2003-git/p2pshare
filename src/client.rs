// Partie 3 — Client de téléchargement TCP
//
// • Connexion à un pair
// • LIST  → liste ses fichiers
// • GET   → télécharge avec barre de progression et vérification SHA-256

use crate::protocol::{TcpRequest, TcpResponse};
use anyhow::{anyhow, Result};
use indicatif::{ProgressBar, ProgressStyle};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// Récupérer la liste des fichiers d'un pair
pub async fn list_files(ip: &str, port: u16) -> Result<Vec<crate::protocol::FileInfo>> {
    let mut conn = connect(ip, port).await?;
    send_request(&mut conn, &TcpRequest::List).await?;

    let (reader, _writer) = conn.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    match serde_json::from_str::<TcpResponse>(line.trim())? {
        TcpResponse::FileList { files } => Ok(files),
        other => Err(anyhow!("Réponse inattendue : {:?}", other)),
    }
}

/// Télécharger un fichier depuis un pair vers un dossier local
/// Supporte la reprise si le fichier partiel existe déjà.
pub async fn download_file(
    ip: &str,
    port: u16,
    filename: &str,
    dest_dir: &std::path::Path,
) -> Result<()> {
    let dest_path = dest_dir.join(filename);

    // Offset de reprise
    let offset = if dest_path.exists() {
        tokio::fs::metadata(&dest_path).await?.len()
    } else {
        0
    };

    let mut conn = connect(ip, port).await?;
    send_request(
        &mut conn,
        &TcpRequest::Get {
            filename: filename.to_string(),
            offset,
        },
    )
    .await?;

    // Lire la réponse métadonnées
    let (reader, writer) = conn.split();
    let mut reader = BufReader::new(reader);
    let _ = writer; // pas d'envoi supplémentaire

    let mut line = String::new();
    reader.read_line(&mut line).await?;

    let (total_size, expected_sha256, server_offset) = match serde_json::from_str::<TcpResponse>(line.trim())? {
        TcpResponse::FileStart { size, sha256, offset, .. } => (size, sha256, offset),
        TcpResponse::NotFound { filename } => {
            return Err(anyhow!("Fichier '{}' introuvable sur le pair", filename))
        }
        other => return Err(anyhow!("Réponse inattendue : {:?}", other)),
    };

    // Barre de progression
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})"
        )
        .unwrap()
        .progress_chars("=>-"),
    );
    pb.set_position(server_offset);

    // Ouvrir le fichier destination (append si reprise)
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(offset > 0)
        .write(true)
        .open(&dest_path)
        .await?;

    // Hasher ce qui existe déjà si reprise
    let mut hasher = Sha256::new();
    if offset > 0 {
        let existing = tokio::fs::read(&dest_path).await?;
        hasher.update(&existing);
    }

    // Recevoir les données
    let mut buf = vec![0u8; 65536];
    let mut received = 0u64;
    let to_receive = total_size - server_offset;

    loop {
        let to_read = std::cmp::min(buf.len() as u64, to_receive - received) as usize;
        if to_read == 0 {
            break;
        }
        let n = reader.read(&mut buf[..to_read]).await?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n]).await?;
        hasher.update(&buf[..n]);
        received += n as u64;
        pb.set_position(server_offset + received);
    }

    pb.finish_with_message("Téléchargement terminé");
    file.flush().await?;

    // Vérification de l'intégrité
    let actual_sha256 = hex::encode(hasher.finalize());
    if actual_sha256 == expected_sha256 {
        println!("✅ Intégrité vérifiée : {filename}");
    } else {
        eprintln!("⚠️  Hash SHA-256 incorrect pour '{filename}' — fichier peut être corrompu");
        eprintln!("   Attendu  : {expected_sha256}");
        eprintln!("   Reçu     : {actual_sha256}");
    }

    Ok(())
}

async fn connect(ip: &str, port: u16) -> Result<TcpStream> {
    let addr = format!("{ip}:{port}");
    let stream = TcpStream::connect(&addr).await?;
    Ok(stream)
}

async fn send_request(stream: &mut TcpStream, req: &TcpRequest) -> Result<()> {
    let mut json = serde_json::to_string(req)?;
    json.push('\n');
    stream.write_all(json.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}
