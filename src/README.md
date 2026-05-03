
# P2PShare — Partage de fichiers pair-à-pair en réseau local

Application Rust de partage de fichiers en LAN, sans serveur central.  
Inspirée de BitTorrent, simplifiée pour les réseaux locaux (ENSPD, amphithéâtres, etc.)

---

## Architecture

```
p2pshare/
├── Cargo.toml
└── src/
    ├── main.rs           — Point d'entrée, orchestration des modules
    ├── protocol.rs       — Structures de données partagées (UDP + TCP)
    ├── shared_state.rs   — État global thread-safe (pairs, fichiers)
    ├── discovery.rs      — Partie 1 : annonces UDP multicast
    ├── server.rs         — Partie 2 : serveur TCP de transfert
    ├── client.rs         — Partie 3 : client de téléchargement
    ├── multi_download.rs — Partie 4 : téléchargement multi-sources
    └── cli.rs            — Partie 5 : CLI interactive (REPL)
```

---

## Compilation

```bash
cargo build --release
```

---

## Utilisation

```bash
# Démarrer avec les options par défaut
cargo run -- --name Alice --port 7878 --share-dir ./mes_fichiers

# Sur une autre machine
cargo run -- --name Bob --port 7878 --share-dir ./mes_fichiers
```

### Commandes CLI

| Commande                         | Description                              |
|----------------------------------|------------------------------------------|
| `peers`                          | Lister les pairs actifs sur le réseau    |
| `share`                          | Lister mes fichiers partagés             |
| `files <ip> <port>`              | Voir les fichiers d'un pair              |
| `get <ip> <port> <fichier>`      | Télécharger un fichier (simple)          |
| `mget <fichier> <sha256> <taille>`| Téléchargement multi-sources            |
| `help`                           | Afficher l'aide                          |
| `quit`                           | Quitter                                  |

---

## Protocole

### UDP Multicast (découverte)
- Groupe : `239.255.42.99:7879`
- Paquet JSON envoyé toutes les 5 secondes :
```json
{
  "name": "Alice",
  "ip": "192.168.1.10",
  "tcp_port": 7878,
  "files": [{"name": "cours.pdf", "size": 204800, "sha256": "abc123..."}]
}
```

### TCP (transfert)
Protocole ligne-par-ligne (JSON + `\n`) :

**Requête LIST :**
```json
{"cmd": "LIST"}
```
**Réponse :**
```json
{"type": "file_list", "files": [...]}
```

**Requête GET :**
```json
{"cmd": "GET", "filename": "cours.pdf", "offset": 0}
```
**Réponse :**
```json
{"type": "file_start", "name": "cours.pdf", "size": 204800, "sha256": "...", "offset": 0}
```
Suivi des données binaires brutes.

---

## Dépendances

| Crate       | Usage                          |
|-------------|--------------------------------|
| tokio       | Runtime async + TCP            |
| serde_json  | Sérialisation du protocole     |
| sha2 + hex  | Vérification d'intégrité       |
| indicatif   | Barres de progression          |
| anyhow      | Gestion des erreurs            |
| clap        | Parsing des arguments CLI      |
| chrono      | Horodatage des pairs           |
