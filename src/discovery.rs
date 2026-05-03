// Partie 1 — Découverte de pairs via UDP multicast
//
// Chaque nœud :
//   • Envoie une annonce JSON en multicast toutes les 5 secondes
//   • Écoute les annonces des autres et met à jour la table des pairs

use crate::protocol::AnnouncePacket;
use crate::shared_state::{PeerEntry, SharedState};
use anyhow::Result;
use chrono::Utc;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;

const MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(239, 255, 42, 99);
const MULTICAST_PORT: u16 = 7879;

pub async fn run(state: SharedState) -> Result<()> {
    // Lancer l'émetteur et le récepteur en parallèle
    let send_state = state.clone();
    let recv_state = state.clone();

    let send_handle = tokio::task::spawn_blocking(move || send_loop(send_state));
    let recv_handle = tokio::task::spawn_blocking(move || recv_loop(recv_state));

    // Nettoyage périodique des pairs inactifs
    let evict_state = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            evict_state.evict_stale_peers().await;
        }
    });

    let _ = tokio::join!(send_handle, recv_handle);
    Ok(())
}

/// Émet une annonce multicast toutes les 5 secondes
fn send_loop(state: SharedState) {
    let socket = UdpSocket::bind("0.0.0.0:0").expect("bind émetteur UDP");
    socket
        .set_multicast_ttl_v4(4)
        .expect("set_multicast_ttl_v4");
    let dest = SocketAddr::new(IpAddr::V4(MULTICAST_ADDR), MULTICAST_PORT);

    loop {
        // Récupérer la liste des fichiers locaux de façon bloquante
        let files = tokio::runtime::Handle::current()
            .block_on(state.list_local_files());

        // Récupérer l'IP locale (approximation)
        let local_ip = get_local_ip().unwrap_or_else(|| "127.0.0.1".to_string());

        let packet = AnnouncePacket {
            name: state.name().to_string(),
            ip: local_ip,
            tcp_port: state.tcp_port(),
            files,
        };

        if let Ok(json) = serde_json::to_vec(&packet) {
            let _ = socket.send_to(&json, dest);
        }

        std::thread::sleep(Duration::from_secs(5));
    }
}

/// Reçoit les annonces des autres pairs
fn recv_loop(state: SharedState) {
    let socket = UdpSocket::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        MULTICAST_PORT,
    ))
    .expect("bind récepteur UDP");

    socket
        .join_multicast_v4(&MULTICAST_ADDR, &Ipv4Addr::UNSPECIFIED)
        .expect("join_multicast_v4");

    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let mut buf = vec![0u8; 65536];
    loop {
        match socket.recv_from(&mut buf) {
            Ok((n, src)) => {
                if let Ok(packet) = serde_json::from_slice::<AnnouncePacket>(&buf[..n]) {
                    let key = format!("{}:{}", packet.ip, packet.tcp_port);
                    // Ignorer sa propre annonce
                    let local_ip = get_local_ip().unwrap_or_default();
                    if packet.ip == local_ip && packet.tcp_port == state.tcp_port() {
                        continue;
                    }
                    let _ = src; // adresse source disponible si besoin
                    let entry = PeerEntry {
                        packet,
                        last_seen: Utc::now(),
                    };
                    tokio::runtime::Handle::current()
                        .block_on(state.upsert_peer(key, entry));
                }
            }
            Err(_) => {} // timeout ou erreur réseau, continuer
        }
    }
}

/// Tente de déduire l'IP locale non-loopback
fn get_local_ip() -> Option<String> {
    // Astuce UDP : connexion fictive vers une adresse externe révèle l'IP locale
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    let addr = socket.local_addr().ok()?;
    Some(addr.ip().to_string())
}
