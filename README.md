# ORCHESTRA

Plateforme intégrée d'orchestration : conteneurs, machines virtuelles et inférence IA,
avec un cœur de contrôle en Rust.

> **État : fondations (phases 0–1, début de la phase 7).** Ce dépôt contient un control
> plane fonctionnel de bout en bout et sécurisé par mTLS, mais le runtime des agents est
> **simulé** : aucun conteneur ni VM n'est réellement lancé. Rien ici n'est prêt pour la
> production — voir [Ce qui n'existe pas encore](#ce-qui-nexiste-pas-encore).

## Ce qui fonctionne

- **Modèle de ressources versionné** (`orchestra.io/v1`) : `Node`, `Workload` (conteneur
  ou VM, tous deux de premier rang), `AIWorkload`, `Instance`.
- **Moteur d'état** : store versionné, concurrence optimiste, notifications de changement,
  journal d'écriture (WAL) durable avec compaction et reprise après crash.
- **Scheduler** : fonction pure et déterministe ; filtrage CPU/mémoire/sélecteurs,
  allocation exclusive des GPU, affinité NUMA, bin-packing des charges GPU, répartition
  des charges CPU hors des nœuds GPU.
- **Contrôleurs de réconciliation** idempotents : réplicas, remplacement des instances en
  échec, détection de perte de nœud et replanification, traduction `AIWorkload` →
  `Workload`, garbage collection par propriétaire.
- **Sécurité** : mTLS obligatoire, PKI interne, enrôlement des nœuds par jeton, rotation
  automatique des certificats, autorisation par identité, journal d'audit — voir
  [Sécurité](#sécurité).
- **API REST native** (Axum), **agent de nœud** et **CLI** `orchestra`.

## Démarrage rapide

```bash
cargo build
```

Control plane (API HTTPS sur `127.0.0.1:7600`, données dans `./orchestra-data`). Au
premier démarrage il crée l'autorité de certification du cluster et un certificat
administrateur dans `orchestra-data/pki/` :

```bash
cargo run -p orchestra-server
```

Créer un jeton d'enrôlement (valable 1 h) — la CLI utilise par défaut le certificat
administrateur de `orchestra-data/pki/` :

```bash
cargo run -p orchestra-cli -- token create
```

Deux agents, dans d'autres terminaux — l'un annonce 4 GPU simulés. Le jeton ne sert qu'au
premier démarrage ; l'agent conserve ensuite son certificat dans `orchestra-agent-data/` :

```bash
cargo run -p orchestra-agent -- --name gpu-a --join-token <JETON> --cpu 32 --memory 256Gi --simulate-gpu nvidia:4:80Gi:H100
```

```bash
cargo run -p orchestra-agent -- --name cpu-a --join-token <JETON>
```

Déployer et observer :

```bash
cargo run -p orchestra-cli -- apply -f examples/web.yaml
cargo run -p orchestra-cli -- apply -f examples/llm.yaml
cargo run -p orchestra-cli -- get nodes
cargo run -p orchestra-cli -- get workloads
cargo run -p orchestra-cli -- get instances
```

`examples/llm.yaml` est le manifeste `AIWorkload` de l'architecture de référence : chaque
réplica réserve 4 GPU de 80 Gi sur un même nœud. Arrêtez un agent : après le délai de
grâce (`--node-grace-secs`, 20 s par défaut) ses instances sont replanifiées ailleurs, ou
restent `Pending` avec la raison par nœud.

## Sécurité

Toute communication passe par TLS, et chaque requête est attribuée à l'identité portée par
le certificat client, vérifié contre l'autorité de certification du cluster.

| Identité | Obtenue par | Droits |
| --- | --- | --- |
| Administrateur | `pki/admin.crt` créé au premier démarrage du serveur | Tout |
| Nœud | Enrôlement avec un jeton | Son propre heartbeat, ses propres instances, le renouvellement de son certificat |
| Anonyme | — | `/healthz` et l'échange d'un jeton contre un certificat |

- **Enrôlement** : `orchestra token create` produit un jeton à durée limitée (seul son
  hash est stocké). L'agent génère sa clé privée localement et envoie une demande de
  signature ; la clé ne quitte jamais la machine. Un jeton ne permet pas de prendre le nom
  d'un nœud déjà enrôlé.
- **Rotation** : les certificats de nœud durent 24 h (`--node-cert-ttl-secs`) et l'agent
  les renouvelle seul, avec une nouvelle clé, quand il reste moins d'un tiers de leur durée.
- **Éviction** : `orchestra delete node <nom>` retire un nœud. Son agent arrête ses
  instances et s'arrête ; son certificat ne peut plus être renouvelé.
- **Audit** : `orchestra-data/audit.log` reçoit une ligne JSON par modification et par
  accès refusé, avec l'identité de l'appelant.

Pour un serveur joignable depuis d'autres machines : `--listen 0.0.0.0:7600 --tls-san
<nom ou IP>`, puis copier `orchestra-data/pki/ca.crt` sur chaque nœud (`--ca-cert`).

Limites actuelles :

- Pas de révocation : un certificat de nœud évincé reste valide jusqu'à son expiration
  (24 h au plus), mais le serveur refuse ses heartbeats dès l'éviction.
- Un seul rôle administrateur, pas de RBAC par namespace ni d'utilisateurs (Keycloak/OIDC).
- La clé de l'autorité de certification est un fichier sur le disque du serveur (pas
  d'OpenBao/HSM), et l'état n'est pas chiffré au repos.
- Le certificat du serveur dure un an et n'est réémis qu'au redémarrage.
- Un nœud dont le certificat a expiré (agent arrêté plus de 24 h) doit être supprimé puis
  ré-enrôlé.

## Organisation

```text
crates/
  orchestra-domain      contrats : ressources, quantités (500m, 32Gi), validation
  orchestra-state       moteur d'état + WAL (futur point d'ancrage du consensus Raft)
  orchestra-scheduler   placement, y compris GPU et topologie
  orchestra-controller  boucles de réconciliation
  orchestra-pki         autorité de certification, identités, émission et rotation
  orchestra-api         API REST native /api/v1, autorisation, audit
apps/
  orchestra-server      control plane (transport mTLS)
  orchestra-agent       agent de nœud (trait Runtime + runtime simulé)
  orchestra-cli         binaire `orchestra`
examples/               manifestes
```

## Principes de conception

- **Niveau, pas front** : contrôleurs et agents relisent l'état désiré et convergent ; une
  passe sur un cluster convergé n'écrit rien. Toute étape peut être rejouée après un crash.
- **L'agent est autonome** : si le control plane est injoignable, il ne touche à rien et
  les workloads continuent de tourner. Symétriquement, un redémarrage du control plane ne
  déclare aucun nœud perdu avant un délai de grâce complet.
- **Pas de runtime ni d'hyperviseur maison** : le trait `Runtime` de l'agent est la
  frontière où brancher containerd et Cloud Hypervisor.

## Tests

```bash
cargo test
```

## Ce qui n'existe pas encore

| Domaine | Manque |
| --- | --- |
| Sécurité | RBAC fin, utilisateurs (Keycloak/OIDC), secrets (OpenBao), révocation, chiffrement au repos |
| Compute | Exécution réelle (containerd/CRI, KVM/Cloud Hypervisor) ; découverte réelle des GPU |
| HA | Consensus Raft : le control plane est mono-instance |
| Kubernetes | API et contrôleurs de compatibilité |
| IA | Passerelle d'inférence, autoscaling (les réplicas restent à `minReplicas`), registre de modèles, MIG, placement multi-nœuds |
| Déploiement | Mises à jour progressives (un changement de template recrée toutes les instances), back-off sur échec de démarrage |
| Réseau, stockage, observabilité, IaC, air-gap | Non commencés |
