// ============================================================
// P2PShare — Point d'entrée principal
// Auteur    : NZIE NZOUANGO MARC VICTOR — 22G00348
// Encadrant : Dr MIMBEU — ENSPD 2025-2026
// ------------------------------------------------------------
// Ce module orchestre trois services en parallèle :
//   1. discovery : annonces UDP multicast toutes les 5s
//   2. server    : serveur TCP de transfert de fichiers
//   3. cli       : interface interactive REPL
// =============================================================
mod discovery;
mod server;
mod client;
mod multi_download;
mod cli;
mod protocol;
mod shared_state;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "p2pshare", about = "Partage de fichiers P2P en réseau local")]
struct Args {
    /// Dossier des fichiers à partager
    #[arg(short, long, default_value = "./shared")]
    share_dir: PathBuf,

    /// Port TCP du serveur de transfert
    #[arg(short, long, default_value_t = 7878)]
    port: u16,

    /// Nom affiché sur le réseau
    #[arg(short, long, default_value = "peer")]
    name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Créer le dossier partagé s'il n'existe pas
    tokio::fs::create_dir_all(&args.share_dir).await?;

    println!("🚀 P2PShare démarré — {} sur le port {}", args.name, args.port);
    println!("📁 Dossier partagé : {}", args.share_dir.display());

    // État partagé entre les modules
    let state = shared_state::SharedState::new(args.share_dir.clone(), args.name.clone(), args.port);

    // Lancer la découverte UDP en arrière-plan
    let discovery_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = discovery::run(discovery_state).await {
            eprintln!("❌ Erreur découverte : {e}");
        }
    });

    // Lancer le serveur TCP en arrière-plan
    let server_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = server::run(server_state).await {
            eprintln!("❌ Erreur serveur : {e}");
        }
    });

    // Lancer la CLI interactive
    cli::run(state).await?;

    Ok(())
}
