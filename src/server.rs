// Partie 2 — Serveur TCP de transfert de fichiers
//
// Protocole :
//   Client → JSON ligne (TcpRequest) \n
//   Serveur → JSON ligne (TcpResponse) \n
//   [Si GET réussi] Serveur → données binaires brutes du fichier (depuis offset)

use crate::protocol::{TcpRequest, TcpResponse};
use crate::shared_state::SharedState;
use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

pub async fn run(state: SharedState) -> Result<()> {
    let addr = format!("0.0.0.0:{}", state.tcp_port());
    let listener = TcpListener::bind(&addr).await?;
    println!("🖥️  Serveur TCP en écoute sur {addr}");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, peer_addr.to_string(), state).await {
                eprintln!("Erreur client {peer_addr} : {e}");
            }
        });
    }
}

async fn handle_client(
    mut stream: tokio::net::TcpStream,
    peer_addr: String,
    state: SharedState,
) -> Result<()> {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break; // connexion fermée
        }

        let request: TcpRequest = match serde_json::from_str(line.trim()) {
            Ok(r) => r,
            Err(e) => {
                let resp = TcpResponse::Error {
                    message: format!("Requête invalide : {e}"),
                };
                send_response(&mut writer, &resp).await?;
                continue;
            }
        };

        match request {
            TcpRequest::List => {
                let files = state.list_local_files().await;
                let resp = TcpResponse::FileList { files };
                send_response(&mut writer, &resp).await?;
            }

            TcpRequest::Get { filename, offset } => {
                let path = state.share_dir().join(&filename);

                if !path.exists() || !path.is_file() {
                    let resp = TcpResponse::NotFound { filename };
                    send_response(&mut writer, &resp).await?;
                    continue;
                }

                // Trouver les infos du fichier
                let files = state.list_local_files().await;
                let info = files.iter().find(|f| f.name == filename);

                let (size, sha256) = match info {
                    Some(f) => (f.size, f.sha256.clone()),
                    None => {
                        let resp = TcpResponse::NotFound { filename };
                        send_response(&mut writer, &resp).await?;
                        continue;
                    }
                };

                // Envoyer les métadonnées
                let resp = TcpResponse::FileStart {
                    name: filename.clone(),
                    size,
                    sha256,
                    offset,
                };
                send_response(&mut writer, &resp).await?;

                // Ouvrir le fichier et se positionner à l'offset
                let mut file = tokio::fs::File::open(&path).await?;
                if offset > 0 {
                    use tokio::io::AsyncSeekExt;
                    file.seek(std::io::SeekFrom::Start(offset)).await?;
                }

                // Transférer en streaming
                let mut buf = vec![0u8; 65536];
                let mut sent = offset;
                loop {
                    let n = file.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    writer.write_all(&buf[..n]).await?;
                    sent += n as u64;
                }
                writer.flush().await?;

                println!(
                    "✅ [{peer_addr}] Envoyé '{filename}' ({} octets)",
                    sent - offset
                );
            }
        }
    }

    Ok(())
}

async fn send_response(
    writer: &mut (impl AsyncWriteExt + Unpin),
    resp: &TcpResponse,
) -> Result<()> {
    let mut json = serde_json::to_string(resp)?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}
