use crate::protocol::{ChunkTask, TcpRequest, TcpResponse};
use crate::shared_state::SharedState;
use anyhow::{anyhow, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

const CHUNK_SIZE: u64 = 2 * 1024 * 1024;

struct ChunkResult {
    index: usize,
    data: Vec<u8>,
}

pub async fn multi_download(
    state: &SharedState,
    filename: &str,
    total_size: u64,
    expected_sha256: &str,
    peers: Vec<(String, u16)>,
    dest_dir: &Path,
) -> Result<()> {
    if peers.is_empty() {
        return Err(anyhow!("Aucun pair disponible pour le téléchargement"));
    }

    let chunks = make_chunks(filename, total_size, &peers);
    let total_chunks = chunks.len();
    println!(
        "📦 {} chunks × {} Mo — {} pairs",
        total_chunks,
        CHUNK_SIZE / 1024 / 1024,
        peers.len()
    );

    let mp = MultiProgress::new();
    let style = ProgressStyle::with_template(
        "{spinner:.blue} [{bar:30.cyan/blue}] {bytes}/{total_bytes} — {msg}",
    )
    .unwrap()
    .progress_chars("=>-");

    let (tx, mut rx) = mpsc::channel::<Result<ChunkResult>>(total_chunks);

    for task in chunks {
        let tx = tx.clone();
        let pb = mp.add(ProgressBar::new(task.length));
        pb.set_style(style.clone());
        pb.set_message(format!("chunk #{} de {}", task.index, task.peer_ip));

        tokio::spawn(async move {
            let result = download_chunk(&task, pb).await;
            let _ = tx.send(result).await;
        });
    }
    drop(tx);

    let mut chunks_data: Vec<Option<Vec<u8>>> = vec![None; total_chunks];
    let mut errors = 0;

    while let Some(result) = rx.recv().await {
        match result {
            Ok(cr) => {
                chunks_data[cr.index] = Some(cr.data);
            }
            Err(e) => {
                eprintln!("Erreur chunk : {e}");
                errors += 1;
            }
        }
    }

    if errors > 0 {
        return Err(anyhow!(
            "{errors} chunk(s) ont échoué — téléchargement incomplet"
        ));
    }

    let dest_path = dest_dir.join(filename);
    let mut file = tokio::fs::File::create(&dest_path).await?;
    let mut hasher = Sha256::new();

    for (i, chunk) in chunks_data.into_iter().enumerate() {
        let data = chunk.ok_or_else(|| anyhow!("Chunk #{i} manquant"))?;
        file.write_all(&data).await?;
        hasher.update(&data);
    }
    file.flush().await?;

    let actual = hex::encode(hasher.finalize());
    if actual == expected_sha256 {
        println!("Fichier réassemblé et intégrité vérifiée : {filename}");
    } else {
        eprintln!("Hash incorrect — fichier corrompu");
        eprintln!("   Attendu : {expected_sha256}");
        eprintln!("   Reçu    : {actual}");
    }

    Ok(())
}

//permet de télécharger un chunk depuis un pair donné, avec une barre de progression
async fn download_chunk(task: &ChunkTask, pb: ProgressBar) -> Result<ChunkResult> {
    let addr = format!("{}:{}", task.peer_ip, task.peer_port);
    let mut stream = TcpStream::connect(&addr).await?;

    let req = TcpRequest::Get {
        filename: task.filename.clone(),
        offset: task.offset,
    };
    let mut json = serde_json::to_string(&req)?;
    json.push('\n');
    stream.write_all(json.as_bytes()).await?;
    stream.flush().await?;

    //permet de lire la réponse du serveur avant de commencer à lire les données du chunk
    let (reader_half, _) = stream.split();
    let mut reader = BufReader::new(reader_half);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    match serde_json::from_str::<TcpResponse>(line.trim())? {
        TcpResponse::FileStart { .. } => {}
        TcpResponse::NotFound { filename } => {
            return Err(anyhow!("Fichier '{}' introuvable", filename))
        }
        other => return Err(anyhow!("Réponse inattendue : {:?}", other)),
    }

    let mut data = Vec::with_capacity(task.length as usize);
    let mut buf = vec![0u8; 65536];
    let mut remaining = task.length;

    while remaining > 0 {
        let to_read = std::cmp::min(buf.len() as u64, remaining) as usize;
        let n = reader.read(&mut buf[..to_read]).await?;
        if n == 0 {
            return Err(anyhow!("Connexion coupée avant la fin du chunk"));
        }
        data.extend_from_slice(&buf[..n]);
        remaining -= n as u64;
        pb.inc(n as u64);
    }

    pb.finish_and_clear();
    Ok(ChunkResult {
        index: task.index,
        data,
    })
}

fn make_chunks(filename: &str, total_size: u64, peers: &[(String, u16)]) -> Vec<ChunkTask> {
    let mut tasks = Vec::new();
    let mut offset = 0u64;
    let mut index = 0;

    while offset < total_size {
        let length = std::cmp::min(CHUNK_SIZE, total_size - offset);
        let (ip, port) = &peers[index % peers.len()];

        tasks.push(ChunkTask {
            peer_ip: ip.clone(),
            peer_port: *port,
            filename: filename.to_string(),
            offset,
            length,
            index,
        });

        offset += length;
        index += 1;
    }

    tasks
}
