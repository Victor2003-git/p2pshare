// Partie 5 — CLI interactive (REPL)
//
// Commandes disponibles :
//   peers              — lister les pairs visibles
//   files <ip> <port>  — lister les fichiers d'un pair
//   get <ip> <port> <nom>        — télécharger depuis un pair (simple)
//   mget <nom> <sha256> <taille> — télécharger en multi-sources
//   share              — lister les fichiers locaux partagés
//   help               — afficher l'aide
//   quit               — quitter

use crate::client;
use crate::multi_download;
use crate::shared_state::SharedState;
use anyhow::Result;
use std::io::{self, Write};

pub async fn run(state: SharedState) -> Result<()> {
    println!("\n📡 P2PShare CLI — tapez 'help' pour la liste des commandes\n");

    loop {
        print!("p2p> ");
        io::stdout().flush()?;

        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            break; // EOF
        }

        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        match parts[0] {
            "help" | "h" => print_help(),

            "peers" | "p" => cmd_peers(&state).await,

            "share" | "ls" => cmd_share(&state).await,

            "files" | "f" => {
                if parts.len() < 3 {
                    println!("Usage : files <ip> <port>");
                } else {
                    let ip = parts[1];
                    match parts[2].parse::<u16>() {
                        Ok(port) => cmd_files(ip, port).await,
                        Err(_) => println!("❌ Port invalide"),
                    }
                }
            }

            "get" | "g" => {
                if parts.len() < 4 {
                    println!("Usage : get <ip> <port> <nom_fichier>");
                } else {
                    let ip = parts[1];
                    let filename = parts[3];
                    match parts[2].parse::<u16>() {
                        Ok(port) => cmd_get(&state, ip, port, filename).await,
                        Err(_) => println!("❌ Port invalide"),
                    }
                }
            }

            "mget" | "m" => {
                // mget <nom> <sha256> <taille>
                if parts.len() < 4 {
                    println!("Usage : mget <nom_fichier> <sha256> <taille_octets>");
                } else {
                    let filename = parts[1];
                    let sha256 = parts[2];
                    match parts[3].parse::<u64>() {
                        Ok(size) => cmd_mget(&state, filename, sha256, size).await,
                        Err(_) => println!("❌ Taille invalide"),
                    }
                }
            }

            "quit" | "exit" | "q" => {
                println!("👋 Au revoir !");
                break;
            }

            unknown => println!("❌ Commande inconnue : '{unknown}' — tapez 'help'"),
        }

        println!();
    }

    Ok(())
}

// ─── Commandes ────────────────────────────────────────────────────────────────

fn print_help() {
    println!(
        r#"
┌─────────────────────────────────────────────────────────────────┐
│                    Commandes P2PShare                           │
├──────────────────────────┬──────────────────────────────────────┤
│ peers                    │ Lister les pairs actifs              │
│ files <ip> <port>        │ Fichiers disponibles chez un pair    │
│ get <ip> <port> <nom>    │ Télécharger un fichier               │
│ mget <nom> <sha> <taille>│ Téléchargement multi-sources         │
│ share                    │ Mes fichiers partagés                │
│ help                     │ Afficher cette aide                  │
│ quit                     │ Quitter                              │
└──────────────────────────┴──────────────────────────────────────┘"#
    );
}

async fn cmd_peers(state: &SharedState) {
    let peers = state.get_peers().await;

    if peers.is_empty() {
        println!("⚠️  Aucun pair découvert. Vérifiez votre réseau ou attendez quelques secondes.");
        return;
    }

    println!("👥 {} pair(s) actif(s) :", peers.len());
    println!("{:<20} {:<8} {:<15} {}", "Nom", "Fichiers", "Adresse", "Dernière vue");
    println!("{}", "─".repeat(65));

    for (key, entry) in &peers {
        let elapsed = chrono::Utc::now() - entry.last_seen;
        println!(
            "{:<20} {:<8} {:<15} il y a {}s",
            entry.packet.name,
            entry.packet.files.len(),
            key,
            elapsed.num_seconds()
        );
    }
}

async fn cmd_share(state: &SharedState) {
    let files = state.list_local_files().await;

    if files.is_empty() {
        println!("📂 Aucun fichier partagé. Ajoutez des fichiers dans : {}", state.share_dir().display());
        return;
    }

    println!("📂 {} fichier(s) partagé(s) :", files.len());
    println!("{:<30} {:>12} {}", "Nom", "Taille", "SHA-256 (8 premiers)");
    println!("{}", "─".repeat(70));

    for f in &files {
        println!(
            "{:<30} {:>12} {}",
            f.name,
            format_size(f.size),
            &f.sha256[..8]
        );
    }
}

async fn cmd_files(ip: &str, port: u16) {
    match client::list_files(ip, port).await {
        Ok(files) => {
            if files.is_empty() {
                println!("📂 Le pair ne partage aucun fichier.");
                return;
            }
            println!("📂 {} fichier(s) disponible(s) :", files.len());
            println!("{:<30} {:>12} {}", "Nom", "Taille", "SHA-256 (8 premiers)");
            println!("{}", "─".repeat(70));
            for f in &files {
                println!(
                    "{:<30} {:>12} {}...",
                    f.name,
                    format_size(f.size),
                    &f.sha256[..8]
                );
            }
        }
        Err(e) => println!("❌ Erreur : {e}"),
    }
}

async fn cmd_get(state: &SharedState, ip: &str, port: u16, filename: &str) {
    println!("⬇️  Téléchargement de '{filename}' depuis {ip}:{port}…");
    match client::download_file(ip, port, filename, state.share_dir()).await {
        Ok(()) => println!("✅ Téléchargement terminé → {}", state.share_dir().display()),
        Err(e) => println!("❌ Erreur : {e}"),
    }
}

async fn cmd_mget(state: &SharedState, filename: &str, sha256: &str, size: u64) {
    let peers_map = state.get_peers().await;
    let peers: Vec<(String, u16)> = peers_map
        .values()
        .map(|e| (e.packet.ip.clone(), e.packet.tcp_port))
        .collect();

    if peers.is_empty() {
        println!("⚠️  Aucun pair pour le téléchargement multi-sources.");
        return;
    }

    println!(
        "⬇️  Multi-sources : '{filename}' depuis {} pair(s)…",
        peers.len()
    );

    match multi_download::multi_download(state, filename, size, sha256, peers, state.share_dir()).await {
        Ok(()) => println!("✅ Assemblage terminé → {}", state.share_dir().display()),
        Err(e) => println!("❌ Erreur : {e}"),
    }
}

// ─── Utilitaires ──────────────────────────────────────────────────────────────

fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} Go", bytes as f64 / 1073741824.0)
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} Mo", bytes as f64 / 1048576.0)
    } else if bytes >= 1024 {
        format!("{:.1} Ko", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} o")
    }
}
