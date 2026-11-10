# ORCHESTRA

Plateforme intégrée d'orchestration : conteneurs, machines virtuelles et inférence IA,
avec un cœur de contrôle en Rust.

> **État : fondations (phases 0–1 de la feuille de route).** Ce dépôt contient un control
> plane fonctionnel de bout en bout, mais le runtime des agents est **simulé** et l'API
> n'est **pas authentifiée**. Rien ici n'est prêt pour la production — voir
> [Ce qui n'existe pas encore](#ce-qui-nexiste-pas-encore).

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
- **API REST native** (Axum), **agent de nœud** et **CLI** `orchestra`.

## Démarrage rapide

```bash
cargo build
```

Control plane (API sur `127.0.0.1:7600`, état dans `./orchestra-data`) :

```bash
cargo run -p orchestra-server
```

Deux agents, dans d'autres terminaux — l'un annonce 4 GPU simulés :

```bash
cargo run -p orchestra-agent -- --name gpu-a --cpu 32 --memory 256Gi --simulate-gpu nvidia:4:80Gi:H100
```

```bash
cargo run -p orchestra-agent -- --name cpu-a
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

## Organisation

```text
crates/
  orchestra-domain      contrats : ressources, quantités (500m, 32Gi), validation
  orchestra-state       moteur d'état + WAL (futur point d'ancrage du consensus Raft)
  orchestra-scheduler   placement, y compris GPU et topologie
  orchestra-controller  boucles de réconciliation
  orchestra-api         API REST native /api/v1
apps/
  orchestra-server      control plane
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
| Sécurité | Authentification, RBAC, mTLS, audit — l'API doit rester sur une interface de confiance |
| Compute | Exécution réelle (containerd/CRI, KVM/Cloud Hypervisor) ; découverte réelle des GPU |
| HA | Consensus Raft : le control plane est mono-instance |
| Kubernetes | API et contrôleurs de compatibilité |
| IA | Passerelle d'inférence, autoscaling (les réplicas restent à `minReplicas`), registre de modèles, MIG, placement multi-nœuds |
| Déploiement | Mises à jour progressives (un changement de template recrée toutes les instances), back-off sur échec de démarrage |
| Réseau, stockage, observabilité, IaC, air-gap | Non commencés |
