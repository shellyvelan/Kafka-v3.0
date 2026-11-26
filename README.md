# Air-Gapped Kafka on VMs — Confluent Platform 8.x (Apache Kafka 4.x)

A greenfield, 3-rack KRaft Kafka cluster running as `systemd` services on 17 VMs in an air-gapped network, replacing the Kafka 3.9 Docker Swarm cluster. All traffic is TLS with mutual authentication (mTLS), MongoDB is integrated through Kafka Connect, and all day-2 operations run from a single ops node with a standby.

> **Status:** design agreed. Scripts, templates and units described below are not yet implemented.

---

## 1. Design Decisions (summary)

| Topic | Decision |
| :--- | :--- |
| Migration | **New cluster, no data copy.** Clients are cut over; old cluster is retired. |
| Versions | Confluent Platform 8.x / Apache Kafka 4.x, JDK 21. Exact versions pinned in `offline-bundle/versions.env`. |
| Controllers | **5 dedicated KRaft controllers**, `1 + 2 + 2` across racks. Controller VMs run **only** `kafka-controller` as a service, plus `rhel-agent` (port 6000) on `ctrl-1`. `ctrl-2`/`ctrl-4` also hold the ops scripts (no service). |
| Brokers | 9 dedicated brokers, 3 per rack, rack-aware placement. |
| Connect VMs | **3 dedicated VMs, one per rack**, hosting Connect + all ecosystem services. |
| REST Proxy | 2 instances (`connect-2`, `connect-3`). Clients use one hostname; failover is a manual DNS repoint. |
| Monitoring | `danielqsj/kafka_exporter` **v1.9.0** ×2 + **Prometheus** ×2 on `connect-2`/`connect-3`. Existing Grafana connects to them. |
| Kafbat UI | Runs in **Kubernetes** (Deployment, `replicas: 1`); k8s reschedules it on failure. Not on the VMs. |
| Auth | **mTLS only** (`ssl.client.auth=required`), principals from cert CN, `StandardAuthorizer` ACLs. |
| Certificates | Self-signed cluster CA, **per-host keystores**. |
| Cross-zone ops | RHEL ↔ Ubuntu SSH is blocked; a hardened **`rhel-agent`** (fixed actions only) on `ctrl-1` executes Rack 1 operations. |
| Ops node | **`ctrl-2`** (main) and **`ctrl-4`** (secondary), kept in sync with `ops-sync.sh`. |
| Config generation | `inventory.conf` + `envsubst` templates (pure bash, no Ansible). |
| Storage | Every VM has a single 100 GB `/data` disk: Kafka data in `/data/kafka`, logs in `/data/log/kafka`. |
| Retention | Default **72 h** plus a per-topic `retention.bytes` cap. |

---

## 2. Topology

### 2.1 VM placement (17 VMs, three separate groups)

Every VM belongs to exactly **one** group. Brokers, controllers and Connect VMs never share a machine.

| Group | VMs | Runs | Rack 1 (RHEL 9) | Rack 2 (Ubuntu 22.04) | Rack 3 (Ubuntu 22.04) |
| :--- | :---: | :--- | :--- | :--- | :--- |
| **Brokers** | 9 | `kafka-broker` only | `broker-1`, `broker-2`, `broker-3` | `broker-4`, `broker-5`, `broker-6` | `broker-7`, `broker-8`, `broker-9` |
| **Controllers** | 5 | `kafka-controller` only | `ctrl-1` | `ctrl-2`, `ctrl-3` | `ctrl-4`, `ctrl-5` |
| **Connect / ecosystem** | 3 | Connect, REST Proxy, kafka_exporter, Prometheus | `connect-1`: Connect | `connect-2`: Connect, REST, exporter, Prometheus | `connect-3`: Connect, REST, exporter, Prometheus |

Controller VMs run no service other than `kafka-controller`, except `rhel-agent` (port 6000) on `ctrl-1`, which bridges ops traffic into the RHEL zone (§6). The ops scripts (plain bash, no daemon) live on **`ctrl-2` (main)** and **`ctrl-4` (secondary)**.

SELinux is enforcing on the RHEL rack.

Off-VM: **Kafbat UI** (Kubernetes), **Grafana** (existing), **MongoDB** (existing), **log service** (Kubernetes, REST Proxy client).

### 2.2 Node IDs and listeners

| Role | `node.id` | Listeners | Port |
| :--- | :--- | :--- | :--- |
| Controllers `ctrl-1..5` | `1..5` | `CONTROLLER` (mTLS) | 9093 |
| Brokers `broker-1..9` | `101..109` | `INTERNAL` (mTLS, inter-broker) | 9092 |
| | | `CLIENT` (mTLS, apps, Connect, REST, k8s) | 9094 |

`controller.quorum.voters=1@ctrl-1:9093,2@ctrl-2:9093,3@ctrl-3:9093,4@ctrl-4:9093,5@ctrl-5:9093` (static quorum; the voter set is fixed at format time).

### 2.3 Failure tolerance

| Failure | KRaft quorum (need 3/5) | Data (RF=3, `min.insync.replicas=2`) |
| :--- | :--- | :--- |
| Any 1 controller | ✅ 4/5 | — |
| Rack 1 lost | ✅ 4/5 | ✅ 2 replicas remain, writes continue |
| Rack 2 **or** rack 3 lost | ✅ 3/5 | ✅ 2 replicas remain, writes continue |
| Rack 2/3 lost **+** one more controller | ❌ 2/5, cluster stops accepting metadata changes | — |
| 2 brokers in different racks | ✅ | ⚠️ partitions with replicas on both go below `min.insync.replicas`; `acks=all` writes fail for those |

---

## 3. Services per VM

| VM | systemd units | Heap (default, tunable in inventory) |
| :--- | :--- | :--- |
| `broker-*` | `kafka-broker` | 6 GB |
| `ctrl-*` | `kafka-controller` (+ `rhel-agent` on `ctrl-1`; ops scripts on `ctrl-2`/`ctrl-4`, no unit) | 1 GB |
| `connect-1` | `kafka-connect` | 3 GB |
| `connect-2`, `connect-3` | `kafka-connect`, `kafka-rest`, `kafka-exporter`, `prometheus` | Connect 3 GB, REST 1 GB |

Sizing is unknown at design time; heaps and retention are inventory variables and should be revisited after the first weeks of production traffic.

---

## 4. Component Details

### 4.1 Brokers
* `broker.rack=rack1|rack2|rack3`. The replica placer spreads RF=3 replicas across all three racks.
* `default.replication.factor=3`, `min.insync.replicas=2`, `auto.create.topics.enable=false`, `unclean.leader.election.enable=false`.
* `log.dirs=/data/kafka`, `log.retention.hours=72`.
* **Capacity:** 9 × 100 GB raw at RF=3 gives about **300 GB logical**, minus headroom for logs and segment overhead. Plan on about **250 GB** of retained data in total. Every topic in `topics.conf` must declare `retention.bytes` (per partition) so that `partitions × retention.bytes × 3 / 9` stays below about 70 GB per broker.

### 4.2 Controllers
* `process.roles=controller`, and no other service runs on these VMs except `rhel-agent` on `ctrl-1`. `ctrl-2` and `ctrl-4` also carry the ops scripts, which are tooling and not on the data path.
* Metadata log in `/data/kafka/metadata`.

### 4.3 Kafka Connect (distributed, 3 workers)
* One Connect group (`group.id=connect-cluster`) across `connect-1..3`, so it survives losing a rack.
* Internal topics, created by `create-topics.sh` before Connect starts: `connect-configs` (1 partition, compact), `connect-offsets` (25 partitions, compact), `connect-status` (5 partitions, compact), all RF=3.
* `plugin.path=/opt/confluent/share/java,/opt/connect-plugins`, with the official MongoDB Kafka Connector in `/opt/connect-plugins`.
* Expected **10–50 connectors**. MongoDB source connectors run 1 task each; sink `tasks.max` should not exceed the topic's partition count.
* **Formats (no Schema Registry, no embedded schemas):** set per connector:

  | Payload | Source (`output.format.value`) | Converter |
  | :--- | :--- | :--- |
  | JSON | `json` | `org.apache.kafka.connect.storage.StringConverter` |
  | BSON | `bson` | `org.apache.kafka.connect.converters.ByteArrayConverter` |

* Secrets (`mongo.uri`, keystore passwords) come from `FileConfigProvider`, so they never land in `connect-configs` or Git:
  ```properties
  config.providers=file
  config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
  # in connector JSON:
  "connection.uri": "${file:/etc/kafka/secrets/credentials.properties:mongo.uri}"
  ```
* Connect REST API on **8083 over HTTPS** (cluster CA). Kafbat UI in k8s and the ops node call it.

### 4.4 REST Proxy (2 instances)
* Runs on `connect-2` and `connect-3`, port 8082. Its only consumer is the internal **log service in Kubernetes**, which calls `rest.kafka.<domain>:8082`.
* **Failover is manual:** the DNS name points at `connect-2`; if it fails, repoint it to `connect-3`. This is documented in §8.
* The REST Proxy authenticates to brokers as its own principal (`User:kafka-rest`), so ACLs for everything the log service does are granted to that principal.
* ⚠️ If the log service uses **v2 consumer instances**, those are stateful per proxy instance and are lost on failover. The service must recreate its consumer instance on errors. Producing (v2/v3) is stateless.

### 4.5 Monitoring
* **kafka_exporter v1.9.0** on `connect-2` and `connect-3` (port 9308) exposes consumer lag, offsets, partition, ISR and broker counts. It connects with mTLS as `User:kafka-exporter` (Describe-only ACLs).
* **Prometheus** on `connect-2` and `connect-3` (port 9090). **Each Prometheus scrapes both exporters**, so either one alone has full data. Series carry an `instance` label. Grafana dashboards aggregate with `max by (topic, partition, consumergroup)` to avoid double-counting.
* Prometheus TSDB in `/data/prometheus`, `--storage.tsdb.retention.time=15d --storage.tsdb.retention.size=15GB`.
* The existing **Grafana** gets two datasources (`prometheus-r2`, `prometheus-r3`). Point dashboards at r2 and switch to r3 if r2 is down.
* **Accepted gap:** no JMX or host metrics. Disk usage on the 100 GB `/data` disks is therefore **not** monitored by Prometheus, and `health-check.sh` reports it instead (see §10).

### 4.6 Kafbat UI (Kubernetes)
* Image is pushed to the internal registry from the offline bundle (`offline-bundle/images/kafbat-ui.tar`).
* `Deployment` with `replicas: 1`. Kubernetes restarts or reschedules it on pod or node failure, so no failover logic is needed.
* Keystore/truststore live in a k8s `Secret`, and the principal is `User:kafbat-ui`. ACLs are read-only by default.
* Needs network access from k8s to brokers `:9094` and Connect `:8083`.
* Manifests live in `k8s/kafbat-ui/`.

---

## 5. Security

### 5.1 TLS and principals
* `certs/generate-certs.sh` creates a self-signed **cluster CA** and one **per-host keystore** (PKCS12) for every VM. SANs are hostname + FQDN + IP, and hostname verification is enabled.
* The **CN identifies the role, not the host**, so every host of a role maps to one principal while each host still has its own key:

  | Cert | CN → principal | Used by |
  | :--- | :--- | :--- |
  | brokers | `kafka-broker` | `broker-*` |
  | controllers | `kafka-controller` | `ctrl-*` |
  | connect | `kafka-connect` | `connect-*` |
  | rest | `kafka-rest` | REST Proxy |
  | exporter | `kafka-exporter` | kafka_exporter |
  | admin | `kafka-admin` | ops scripts on `ctrl-2`/`ctrl-4` |
  | kafbat | `kafbat-ui` | Kafbat UI (k8s) |
  | apps | `<app-name>` | k8s apps connecting natively |

* `ssl.principal.mapping.rules=RULE:^CN=([^,]+).*$/$1/,DEFAULT`
* `super.users=User:kafka-broker;User:kafka-controller;User:kafka-admin`
* `allow.everyone.if.no.acl.found=false`
* Certificates have a fixed validity period; `rotate-certs.sh` re-issues them and does a rolling restart (see §7).

### 5.2 Secrets on disk
Docker secrets are replaced by root-provisioned files owned `kafka:kafka`, mode `600`:
* `/etc/kafka/certs/` contains `keystore.p12` and `truststore.p12`
* `/etc/kafka/secrets/credentials.properties` holds keystore passwords and `mongo.uri` (used via `FileConfigProvider`)
* `/etc/kafka/secrets/service.env` holds env vars for REST, exporter, Prometheus and the agent (`AGENT_API_TOKEN`), loaded via systemd `EnvironmentFile=`

Real secrets and generated certs are **never committed** (`.gitignore`).

### 5.3 SELinux (RHEL rack)
SELinux stays **enforcing**. Since `/data` and `/opt/*` are non-default paths, `setup-node.sh` applies file contexts (`semanage fcontext` + `restorecon`) and opens ports with `semanage port` where needed. It must never call `setenforce 0`.

---

## 6. Operations Model

### 6.1 Ops nodes
* **Main: `ctrl-2`** (rack 2). **Secondary: `ctrl-4`** (rack 3). Both are Ubuntu, so both can SSH to every Ubuntu VM, and they sit on different racks.
* Both have the same Git checkout at `/opt/kafka-ops`, the admin client cert, and the agent token.
* **`ops-sync.sh`** (run on the primary after any change) rsyncs `secrets/`, `certs/output/` and `inventory.conf` to the standby over SSH.
* **Failover:** SSH into `ctrl-4` and run the same scripts. Nothing else changes, because the agent allow-lists both nodes.
* Ops scripts must be run from one node at a time. Every script takes a lock (`/var/lock/kafka-ops.lock`) on the node it runs from.

### 6.2 Cross-zone control: `rhel-agent`
RHEL ↔ Ubuntu SSH (22) is blocked. For Rack 1, the ops node calls a small HTTPS agent on **`ctrl-1:6000`**. The agent uses passwordless SSH within the RHEL zone to reach `broker-1..3` and `connect-1`.

Hardening:
* **Fixed actions only:** every endpoint maps to a hardcoded command. Request fields are validated against enums and never interpolated into a shell.
* HTTPS with the cluster CA, `Authorization: Bearer $AGENT_API_TOKEN`.
* Firewall and in-app IP allow-list: **`ctrl-2` and `ctrl-4` only**.
* Runs as an unprivileged `kafka-agent` user with sudo rights limited to `systemctl {start,stop,restart,is-active} kafka-*`.
* Python 3 stdlib only, with an audit log in `/data/log/kafka/rhel-agent.log`.

| Method & path | Payload | Action on Rack 1 |
| :--- | :--- | :--- |
| `POST /api/v1/start` | `{"tier": "controllers\|brokers\|connect"}` | Start the tier on Rack 1 VMs |
| `POST /api/v1/stop` | `{"tier": "connect\|brokers\|controllers"}` | Gracefully stop the tier |
| `POST /api/v1/restart-node` | `{"host": "<rack1 host from inventory>", "service": "<enum>"}` | Restart one service on one host. Used by rolling restarts, with the ops node doing the waiting between steps. |
| `GET /api/v1/logs` | `?since=2h` | Stream `logs-rack1.tar.gz` |
| `GET /api/v1/health` | — | Agent status + systemd state of every Rack 1 service |

All waiting (quorum, URP=0, Connect task state) is done **by the ops node** through the Kafka and Connect APIs. The agent only executes single steps.

---

## 7. Scripts (run from the ops node)

| Script | Behavior |
| :--- | :--- |
| `cluster-start.sh` | Controllers (all racks) → wait for quorum (≥3/5, warn if <5) → brokers → wait until all 9 registered → Connect → REST, exporters, Prometheus. |
| `cluster-shutdown.sh` | Reverse order: ecosystem → Connect → brokers (controlled shutdown) → controllers. |
| `cluster-restart.sh` | Full stop + start (**downtime**). Prefer rolling restarts. |
| `rolling-restart.sh --tier brokers\|controllers\|connect\|all` | One node at a time. **Brokers:** restart, wait for URP=0. **Controllers:** restart, wait until the node is back in `kafka-metadata-quorum describe --replication` with lag ≈ 0; never restart while another voter is down. **Connect:** restart, wait until every connector and task is `RUNNING`. `all` = controllers → brokers → Connect. Rack 1 nodes go through the agent. |
| `get-logs.sh --since "2 hours ago"` | Pulls `/data/log/kafka` from Ubuntu VMs via SSH and Rack 1 via the agent, and bundles them into one archive. |
| `health-check.sh` | Agent health, quorum, broker count, URP / under-min-ISR, Connect workers and failed tasks, REST/exporter/Prometheus up, **`/data` usage on all 17 VMs** (warn at 70%, critical at 85%). |
| `create-topics.sh` | Idempotent. Applies `configs/topics.conf` (Connect internal topics included). Rejects topics with no `retention.bytes`. |
| `create-acls.sh` | Idempotent. Applies `configs/acls.conf`. |
| `connectors/create-all.sh` | `PUT` each `connectors/*.json` to `/connectors/<name>/config` (create or update). |
| `connectors/restart-all.sh` | `POST /connectors/<name>/restart?includeTasks=true&onlyFailed=…` |
| `connectors/delete-all.sh` | Lists and deletes all connectors (asks for confirmation). |
| `rotate-certs.sh` | Distributes new keystores, then runs a rolling restart of every tier. |
| `ops-sync.sh` | Syncs secrets, certs and inventory from the primary to the standby ops node. |

Bootstrap-only: `build-offline-bundle.sh`, `generate-certs.sh`, `distribute-certs.sh`, `distribute-secrets.sh`, `setup-node.sh`.

---

## 8. Runbooks

### 8.1 Initial deployment (air-gapped)
1. **Build the bundle** on the internet-connected staging host and transfer + verify it on `ctrl-2`, `ctrl-4` and `ctrl-1` (§9). Install the OS packages from the bundle on every VM first.
2. **Secrets:** fill `secrets/credentials.properties` and `secrets/service.env` from the `.example` files, using a long random `AGENT_API_TOKEN`.
3. **Certs:** `./certs/generate-certs.sh` (all 17 hosts + client certs). Keep the CA key offline after use.
4. **Cluster ID:** `kafka-storage random-uuid`, recorded once in `inventory.conf`.
5. **RHEL zone** (on `ctrl-1`): `distribute-certs.sh --zone rhel`, `distribute-secrets.sh --zone rhel`, `setup-node.sh --zone rhel`, then `systemctl enable --now rhel-agent`.
6. **Ubuntu zone** (on `ctrl-2`): the same three scripts with `--zone ubuntu`, then `ops-sync.sh`.
7. **Start:** `cluster-start.sh` (Connect waits) → `create-topics.sh` → `create-acls.sh` → start Connect → `health-check.sh`.
8. **Connectors:** `connectors/create-all.sh`.
9. **k8s:** load the Kafbat image into the registry, create the Secret, apply `k8s/kafbat-ui/`, and add both Prometheus datasources to Grafana.
10. **Cutover:** repoint clients and the log service to the new cluster, then decommission Swarm.

`setup-node.sh` unpacks the binaries, creates the `kafka` user, renders configs from templates, installs units, sets OS tuning (`vm.swappiness=1`, `nofile` ≥ 100000, `vm.max_map_count`), applies SELinux contexts on RHEL, and runs `kafka-storage format` on controllers and brokers. Formatting is skipped if `meta.properties` already exists.

### 8.2 REST Proxy failover
1. Confirm `connect-2`'s REST Proxy is down: `health-check.sh`.
2. Repoint `rest.kafka.<domain>` to `connect-3`.
3. Tell the log-service owners that consumer instances must be recreated.
4. Switch back once `connect-2` is healthy (optional).

### 8.3 Ops node failover
SSH into `ctrl-4` and work from `/opt/kafka-ops`. When `ctrl-2` returns, run `ops-sync.sh` **from `ctrl-4` to `ctrl-2`** before switching back.

### 8.4 Disk pressure on `/data`
1. `health-check.sh` shows which VM and directory is affected (`/data/kafka` vs `/data/log/kafka`).
2. Lower `retention.bytes` / `retention.ms` on the biggest topics (`kafka-configs --alter`) and update `topics.conf` to match.
3. If it's logs, check the Log4j2 caps (§10).

---

## 9. Offline Bundle (everything the air-gapped side needs)

**Rule:** nothing on the air-gapped side may reach the internet: no `dnf`/`apt` repos, no `pip`, no `curl` to public URLs, no image pulls from public registries. Everything is built once on an internet-connected **staging host**, checksummed, and carried over as one archive.

`build-offline-bundle.sh` produces `kafka-offline-bundle-<date>.tar.gz` plus `SHA256SUMS`. All versions and checksums are pinned in `offline-bundle/versions.env`, and the build fails if a download doesn't match its checksum.

### 9.1 Bundle contents

| Path in bundle | Content | Installed on |
| :--- | :--- | :--- |
| `binaries/confluent-8.x.y.tar.gz` | Confluent Platform (Kafka, Connect, REST Proxy, CLI tools) | all VMs (brokers/controllers use only the Kafka parts) |
| `binaries/OpenJDK21U-jdk_x64_linux_*.tar.gz` | JDK 21 (Temurin) | all VMs |
| `binaries/kafka_exporter-1.9.0.linux-amd64.tar.gz` | danielqsj/kafka_exporter | `connect-2`, `connect-3` |
| `binaries/prometheus-<ver>.linux-amd64.tar.gz` | Prometheus | `connect-2`, `connect-3` |
| `connect-plugins/mongodb-kafka-connect-mongodb-<ver>.zip` | Official MongoDB connector (with all dependency jars) | `connect-*` |
| `images/kafbat-ui-<ver>.tar` | `docker save` of the Kafbat UI image | loaded into the internal k8s registry |
| `os-packages/rhel9/*.rpm` | OS tools + all their dependencies (see 9.2) | RHEL VMs |
| `os-packages/ubuntu2204/*.deb` | OS tools + all their dependencies (see 9.2) | Ubuntu VMs |
| `repo/kafka-ops.tar.gz` | This Git repo (`git archive`) + a `git bundle` for the full history | `ctrl-1`, `ctrl-2`, `ctrl-4` |
| `grafana/dashboards/*.json` | Kafka exporter / lag dashboards, pre-downloaded | imported into the existing Grafana |
| `k8s/` | Kafbat manifests (image path rewritten to the internal registry) | k8s |
| `docs/` | Offline copies of the relevant docs (Kafka ops, Connect REST API, MongoDB connector config reference) | ops nodes |
| `SHA256SUMS`, `versions.env` | Integrity + exact versions | all |

### 9.2 OS packages (downloaded with all dependencies)

Packages are fetched on the staging host **inside a container of the exact target OS**, so the dependency set matches what's installed on the VMs: `dnf download --resolve --alldeps` in a `rhel9`/UBI 9 container, and `apt-get download` with dependencies (or `apt-offline`) in an `ubuntu:22.04` container. `setup-node.sh` installs them locally with `dnf install ./os-packages/rhel9/*.rpm` / `apt install ./os-packages/ubuntu2204/*.deb` and **only** installs from the bundle.

| Package | Needed for | RHEL 9 | Ubuntu 22.04 |
| :--- | :--- | :---: | :---: |
| `gettext` (`envsubst`) | config templating | ✅ | ✅ (`gettext-base`) |
| `jq` | parsing Connect / agent JSON | ✅ | ✅ |
| `rsync` | `ops-sync.sh`, distribution | ✅ | ✅ |
| `curl` | Connect REST, agent calls | usually present, bundled anyway | ✅ |
| `openssl` | cert generation/inspection | ✅ | ✅ |
| `python3` | `rhel-agent.py` (stdlib only) | ✅ (`ctrl-1`) | — |
| `policycoreutils-python-utils` (`semanage`) | SELinux contexts/ports | ✅ | — |
| `tar`, `unzip` | unpacking bundle / plugin | ✅ | ✅ |
| `xfsprogs` | only if `/data` must be formatted | optional | optional |

The full list is in `offline-bundle/packages-rhel9.txt` and `packages-ubuntu2204.txt`. `setup-node.sh` checks for every required command **before** changing anything and stops with the name of whatever is missing.

### 9.3 Air-gap rules for the code
* Scripts never call public URLs. All endpoints come from `inventory.conf`.
* Kafbat UI, Prometheus and Connect are configured with no update checks, telemetry or remote plugin loading. Confluent telemetry/metrics reporters are disabled (`confluent.telemetry.enabled=false`).
* Connector plugins are loaded only from `/opt/connect-plugins`, with no Confluent Hub installs.
* The bundle includes the repo, so a VM can be fully rebuilt using only the bundle.

### 9.4 Transfer and verification
1. On staging: `build-offline-bundle.sh` → `sha256sum -c SHA256SUMS`.
2. Carry the archive across using your approved transfer medium or process.
3. On `ctrl-2`, `ctrl-4` and `ctrl-1`: verify `SHA256SUMS` again before unpacking to `/opt/kafka-ops/offline-bundle/`.
4. Upgrades follow the same path: bump `versions.env`, rebuild, transfer, verify, then rolling restart.

---

## 10. Filesystem Layout (all VMs)

| Path | Content |
| :--- | :--- |
| `/opt/java` | JDK 21 |
| `/opt/confluent` | Confluent Platform 8.x (tar.gz, same on RHEL and Ubuntu) |
| `/opt/connect-plugins` | MongoDB connector (Connect VMs) |
| `/opt/kafka-exporter`, `/opt/prometheus` | `connect-2`, `connect-3` |
| `/opt/kafka-ops` | Ops repo checkout (`ctrl-2`, `ctrl-4`); agent (`ctrl-1`) |
| `/etc/kafka/` | Rendered `.properties`, `log4j2.yaml` |
| `/etc/kafka/certs/`, `/etc/kafka/secrets/` | `600`, `kafka:kafka` |
| `/data/kafka` | Broker logs / controller metadata |
| `/data/prometheus` | Prometheus TSDB |
| `/data/log/kafka` | Service logs. Log4j2 `RollingFile`, **100 MB × 10 files per service**, plus GC logs capped the same way. |

Logging uses Log4j2 (`log4j2.yaml`), because Kafka 4.x does not read Log4j 1.x configs.

---

## 11. Network / Firewall

| Port | From → To | Purpose |
| :--- | :--- | :--- |
| 9093 | controllers ↔ controllers, brokers → controllers | KRaft (mTLS) |
| 9092 | brokers ↔ brokers | Inter-broker (mTLS) |
| 9094 | Connect, REST, exporters, ops nodes, **k8s** → brokers | Clients (mTLS) |
| 8083 | ops nodes, connect ↔ connect, **k8s (Kafbat)** → Connect VMs | Connect REST (HTTPS) |
| 8082 | **k8s (log service)** → `connect-2`, `connect-3` | REST Proxy |
| 9308 | Prometheus → exporters (`connect-2`, `connect-3`) | Exporter metrics |
| 9090 | Grafana → `connect-2`, `connect-3` | Prometheus |
| 6000 | **`ctrl-2`, `ctrl-4` → `ctrl-1` only** | `rhel-agent` (HTTPS) |
| 22 | within the Ubuntu zone (ops nodes → all Ubuntu VMs, `ctrl-2` ↔ `ctrl-4`); within the RHEL zone (`ctrl-1` → RHEL VMs) | SSH |
| 27017 | Connect VMs → MongoDB | Connector traffic |

All 17 VMs must resolve each other by hostname (DNS or managed `/etc/hosts`), and the names must match the cert SANs.

---

## 12. Repository Layout

```text
.
├── README.md
├── inventory.conf                 # hosts: name, IP, zone, rack, role, node.id, heap
├── offline-bundle/
│   ├── versions.env               # pinned versions + checksums
│   ├── packages-rhel9.txt         # OS packages to fetch (with deps)
│   └── packages-ubuntu2204.txt
├── certs/generate-certs.sh
├── secrets/*.example
├── configs/
│   ├── controller.properties.tmpl
│   ├── broker.properties.tmpl
│   ├── connect-distributed.properties.tmpl
│   ├── kafka-rest.properties.tmpl
│   ├── prometheus.yml.tmpl
│   ├── log4j2.yaml
│   ├── topics.conf
│   └── acls.conf
├── connectors/*.json
├── systemd/
│   ├── kafka-controller.service   kafka-broker.service   kafka-connect.service
│   ├── kafka-rest.service         kafka-exporter.service prometheus.service
│   └── rhel-agent.service
├── grafana/dashboards/            # dashboards shipped in the bundle
├── k8s/kafbat-ui/                 # Deployment, Service/Ingress, Secret template, config
└── scripts/
    ├── lib/                       # common.sh (inventory parsing, ssh/agent dispatch, locking)
    ├── rhel-agent.py
    ├── build-offline-bundle.sh  setup-node.sh  distribute-certs.sh  distribute-secrets.sh
    ├── cluster-start.sh  cluster-shutdown.sh  cluster-restart.sh  rolling-restart.sh
    ├── health-check.sh  get-logs.sh  create-topics.sh  create-acls.sh
    ├── rotate-certs.sh  ops-sync.sh
    └── connectors/ create-all.sh  restart-all.sh  delete-all.sh
```

---

## 13. Open Items & Risks

1. **Staging host:** needs internet, Docker/Podman (for the RHEL 9 / Ubuntu 22.04 package containers and `docker save`), and a RHEL subscription or UBI repos for the RPM downloads.
2. **Version check:** confirm the exact CP 8.x / Kafka 4.x release and the MongoDB connector version available for the bundle, and that they're compatible with JDK 21.
3. **No host or JMX metrics:** disk-full on the 100 GB disks is the most likely outage, and only `health-check.sh` catches it. Consider adding `node_exporter` later.
4. **100 GB per broker** is small. Every new topic needs a capacity check against §4.1.
5. **REST Proxy failover** is manual, and v2 consumer state is lost on failover (§4.4).
6. **`rhel-agent`** is custom code with control over Rack 1. It needs code review and a security sign-off.
7. **Static quorum:** changing the controller set later needs a planned migration to a dynamic quorum (KIP-853).
8. **CA key custody:** decide where the offline CA key lives and who can issue certs.
