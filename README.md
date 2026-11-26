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
| Monitoring | `danielqsj/kafka_exporter` **v1.9.0** ×2 + **Prometheus** ×2 on `connect-2`/`connect-3`. Existing Grafana connects to them. The exporter comes from the image already on site, not from the bundle. |
| Kafbat UI | Runs in **Kubernetes**, deployed by you from the image already on site. This repo only issues its client cert and ACLs. |
| Listeners | Brokers: **`INTERNAL` 9092** (inter-broker + all VM-side clients) and **`EXTERNAL` 29092** (k8s clients). Controllers 9093, Connect 8083, REST 8082, exporter 9308. |
| Auth | **mTLS only** (`ssl.client.auth=required`), principals from cert CN, `StandardAuthorizer` ACLs. |
| Certificates | Self-signed cluster CA, **per-host keystores**. |
| Cross-zone ops | RHEL ↔ Ubuntu SSH is blocked; a hardened **`rhel-agent`** (fixed actions only) on `ctrl-1` executes Rack 1 operations. |
| Ops node | **`ctrl-2`** (main) and **`ctrl-4`** (secondary), kept in sync with `ops-sync.sh`. |
| Config generation | `inventory.conf` + `envsubst` templates (pure bash, no Ansible). The inventory is the single source of truth for who talks to whom (§2.4). |
| Service accounts | System user `kafka` runs every Kafka/ecosystem unit; `kafka-agent` runs the agent (§5.4). |
| systemd | One unit per service, `enabled` at boot, `Restart=on-failure` with a limit of 3 tries per 10 min (§5.4). |
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

Off-VM (all existing, not managed by this repo): **Kafbat UI** (k8s), **k8s apps** (native clients on 29092), **log service** (k8s, REST Proxy client), **Grafana**, **MongoDB**.

### 2.2 Node IDs and listeners

| Role | `node.id` | Listeners | Port |
| :--- | :--- | :--- | :--- |
| Controllers `ctrl-1..5` | `1..5` | `CONTROLLER` (mTLS) | 9093 |
| Brokers `broker-1..9` | `101..109` | `INTERNAL` (mTLS): inter-broker **and** VM-side clients (Connect, REST, exporter, ops scripts) | 9092 |
| | | `EXTERNAL` (mTLS): clients in **k8s** (apps, Kafbat UI) | 29092 |
| Connect workers | — | REST API (HTTPS) | 8083 |
| REST Proxy | — | **HTTPS** API (own server keystore) | 8082 |
| kafka_exporter | — | `/metrics` | 9308 |
| Prometheus | — | HTTP | 9090 |
| `rhel-agent` | — | HTTPS API | 6000 |

Broker listener config (rendered per broker from the inventory):
```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092
advertised.listeners=INTERNAL://broker-4.<domain>:9092,EXTERNAL://${EXTERNAL_HOST}:29092
listener.security.protocol.map=INTERNAL:SSL,EXTERNAL:SSL,CONTROLLER:SSL
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
```
`EXTERNAL_HOST` is the broker's FQDN. **k8s pods resolve the broker FQDNs through DNS (confirmed)**, so k8s clients bootstrap on `broker-N.<domain>:29092` and follow the advertised names. Broker certs also carry IP SANs, so switching to IP advertising later wouldn't need new certs.

`controller.quorum.voters=1@ctrl-1:9093,2@ctrl-2:9093,3@ctrl-3:9093,4@ctrl-4:9093,5@ctrl-5:9093` (static quorum; the voter set is fixed at format time).

### 2.3 Failure tolerance

| Failure | KRaft quorum (need 3/5) | Data (RF=3, `min.insync.replicas=2`) |
| :--- | :--- | :--- |
| Any 1 controller | ✅ 4/5 | — |
| Rack 1 lost | ✅ 4/5 | ✅ 2 replicas remain, writes continue |
| Rack 2 **or** rack 3 lost | ✅ 3/5 | ✅ 2 replicas remain, writes continue |
| Rack 2/3 lost **+** one more controller | ❌ 2/5, cluster stops accepting metadata changes | — |
| 2 brokers in different racks | ✅ | ⚠️ partitions with replicas on both go below `min.insync.replicas`; `acks=all` writes fail for those |

### 2.4 How VMs know whom to talk to

**Yes, through configuration. Nothing is discovered automatically.** `inventory.conf` is the single source of truth, and every config is rendered from it:

```text
# name       ip          zone    rack   role        node_id  heap  external_host
ctrl-1       10.0.1.11   rhel    rack1  controller  1        1g    -
ctrl-2       10.0.2.11   ubuntu  rack2  controller  2        1g    -
broker-1     10.0.1.21   rhel    rack1  broker      101      6g    broker-1.<domain>
connect-2    10.0.2.31   ubuntu  rack2  connect     -        3g    -
...
```

| Who | Needs to know | Rendered into |
| :--- | :--- | :--- |
| Controllers | all 5 controllers | `controller.quorum.voters` |
| Brokers | all 5 controllers + its own name, rack, ID | `controller.quorum.bootstrap.servers` / voters, `node.id`, `broker.rack`, `advertised.listeners` |
| Connect, REST, exporter, ops scripts | **a few** brokers (`bootstrap.servers`, one per rack). The client learns all other brokers from cluster metadata. | `bootstrap.servers=broker-1:9092,broker-4:9092,broker-7:9092` |
| Connect workers ↔ each other | **nothing**. They find each other through Kafka (same `group.id` + internal topics). Each advertises its own REST address for request forwarding. | `group.id`, `rest.advertised.host.name` |
| Prometheus | both exporters | `prometheus.yml` scrape targets |
| Ops scripts | every host, zone and role | read the inventory directly (SSH for Ubuntu, agent for RHEL) |
| `rhel-agent` | Rack 1 hosts and their services | copy of the inventory (Rack 1 rows only) |
| Every VM | hostname → IP of all 17 VMs | **Corporate DNS** (all 17 FQDNs). `health-check.sh` checks that forward and reverse lookups match the inventory. |

**Changing the topology** (e.g. replacing a VM) means editing `inventory.conf`, re-rendering configs (`setup-node.sh --render-only`), and doing a rolling restart of the affected tiers.

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
* Connects to brokers on `INTERNAL` **9092**.
* Connect REST API on **8083 over HTTPS** (cluster CA). The ops node and Kafbat UI (from k8s) call it.

### 4.4 REST Proxy (2 instances)
* Runs on `connect-2` and `connect-3`, port **8082**, and talks to brokers on 9092. Its only consumer is the internal **log service in Kubernetes**, which calls `https://rest.kafka.<domain>:8082`.
* **Two TLS sides, as in the current Swarm setup:**
  * **Server side** (`ssl.keystore.*`): REST Proxy's own HTTPS listener that the log service connects to. Cert SANs include `rest.kafka.<domain>` **and** both VM names, so the DNS failover in §8.2 doesn't break TLS. **No client cert is required** (`ssl.client.auth=false`, same as Swarm): the log service only needs the cluster CA to trust the server.
  * **Client side** (`client.ssl.keystore.*` / `client.ssl.truststore.*`): REST Proxy's mTLS identity towards the brokers (`CN=kafka-rest`).
  * Both use the same per-host keystore (one cert with the role CN plus the extra SANs), so the paths match the Swarm env vars `KAFKA_REST_SSL_KEYSTORE_LOCATION`, `KAFKA_REST_CLIENT_SSL_KEYSTORE_LOCATION` and `KAFKA_REST_CLIENT_SSL_TRUSTSTORE_LOCATION`. The log service needs the **cluster CA** in its truststore.
* **Failover is manual:** the DNS name points at `connect-2`; if it fails, repoint it to `connect-3`. This is documented in §8.
* The REST Proxy authenticates to brokers as its own principal (`User:kafka-rest`), so ACLs for everything the log service does are granted to that principal.
* ⚠️ If the log service uses **v2 consumer instances**, those are stateful per proxy instance and are lost on failover. The service must recreate its consumer instance on errors. Producing (v2/v3) is stateless.

### 4.5 Monitoring
* **kafka_exporter v1.9.0** on `connect-2` and `connect-3` (port **9308**) exposes consumer lag, offsets, partition, ISR and broker counts. It connects to brokers on 9092 with mTLS as `User:kafka-exporter` (Describe-only ACLs).
* **Source:** the exporter image you already have on site. The image contains a single static binary, which `setup-node.sh` installs to `/opt/kafka-exporter/` and runs under systemd. The binary is extracted once with `docker create` + `docker cp <id>:/bin/kafka_exporter`. No container runtime is needed on the VMs.
* The exporter is a Go program and can't read PKCS12 keystores, so it gets **PEM** files (`ca.pem`, `kafka-exporter.pem`, `kafka-exporter.key`) from `generate-certs.sh`.
* **Prometheus** on `connect-2` and `connect-3` (port 9090). **Each Prometheus scrapes both exporters**, so either one alone has full data. Series carry an `instance` label. Grafana dashboards aggregate with `max by (topic, partition, consumergroup)` to avoid double-counting.
* Prometheus TSDB in `/data/prometheus`, `--storage.tsdb.retention.time=15d --storage.tsdb.retention.size=15GB`.
* The existing **Grafana** gets two datasources (`prometheus-r2`, `prometheus-r3`). Point dashboards at r2 and switch to r3 if r2 is down.
* **Accepted gap:** no JMX or host metrics. Disk usage on the 100 GB `/data` disks is therefore **not** monitored by Prometheus, and `health-check.sh` reports it instead (§7).

### 4.6 Kafbat UI (Kubernetes, yours)
* You deploy and run it in k8s from the image already on site, so it's **not** in the bundle or this repo. Run it as a Deployment with `replicas: 1` so k8s reschedules it on failure.
* What this repo provides: the **shared k8s client cert** (`CN=k8s-clients`, §5.1), its ACLs, and firewall entries. You put the keystore/truststore in a k8s Secret.
* Points at brokers on **`EXTERNAL` 29092** and Connect on **8083**.

---

## 5. Security

### 5.1 TLS and principals
* `certs/generate-certs.sh` creates a self-signed **cluster CA** and one **per-host keystore** (PKCS12) for every VM. SANs are hostname + FQDN + IP (and `external_host` for brokers). Client certs for Go tools (exporter) are also written as PEM.
* **Hostname checks happen in one direction only.** A *client* checks that the *broker's* cert matches the broker hostname it dialed (`ssl.endpoint.identification.algorithm=https`). That's why broker, controller, Connect and REST *server* certs carry SANs. A *broker* never checks a client's hostname. It only checks that the client cert is **signed by a trusted CA** and maps the CN to a principal. **Client certs therefore have no hostname or SAN requirement** and can be used from anywhere, including any k8s pod on any node.
* The **CN identifies the role, not the host**, so every host of a role maps to one principal while each host still has its own key:

  | Cert | CN → principal | Used by |
  | :--- | :--- | :--- |
  | brokers | `kafka-broker` | `broker-*` |
  | controllers | `kafka-controller` | `ctrl-*` |
  | connect | `kafka-connect` | `connect-*` |
  | rest | `kafka-rest` | REST Proxy |
  | exporter | `kafka-exporter` | kafka_exporter |
  | admin | `kafka-admin` | ops scripts on `ctrl-2`/`ctrl-4` |
  | k8s clients (**one shared cert**) | `k8s-clients` | every k8s client: apps, Kafbat UI. No SANs. |

* `ssl.principal.mapping.rules=RULE:^CN=([^,]+).*$/$1/,DEFAULT`
* `super.users=User:kafka-broker;User:kafka-controller;User:kafka-admin`
* `allow.everyone.if.no.acl.found=false`
* **Shared k8s cert, kept as today:** all k8s clients present the same cert, so to Kafka they are **one principal** (`User:k8s-clients`). Its ACLs are the union of what every k8s app needs. You can't restrict one app without restricting all of them, and audit logs can't tell apps apart. Splitting into per-app certs later only means issuing new certs and adding ACLs, with no broker changes.
* **Validity:** CA **10 years**, host and client certs **5 years**. `health-check.sh` warns 90 days before any cert expires, and `rotate-certs.sh` re-issues certs and does a rolling restart (§7).
* **CA private key (`ca.key`):** anyone who has it can create a cert for any name, including `kafka-admin` (super user), so it is the most sensitive file in the setup.
  * Stored on **`ctrl-2`** in `certs/ca/`, **encrypted with a passphrase** (AES-256). It is never stored unencrypted on disk, and the passphrase is never written to a file. Operators type it only when `generate-certs.sh` / `rotate-certs.sh` issue certs.
  * `ops-sync.sh` copies the **encrypted** key to `ctrl-4`, so the secondary ops node can issue certs too. The key is never distributed to any other VM.
  * The passphrase is known to named operators only and kept in your password manager or vault.

### 5.2 Secrets: lifecycle

**1. Created on the main ops node (`ctrl-2`) only.**
* `secrets/credentials.properties` and `secrets/service.env` are filled from the committed `.example` files.
* `certs/output/` is produced by `generate-certs.sh`.
* All of these are `.gitignore`d. **Real secrets never enter Git or the offline bundle.** They're synced to `ctrl-4` by `ops-sync.sh` and carried to `ctrl-1` for the RHEL zone over the same approved transfer path as the bundle.

**2. Distributed, each host getting only what it needs.** `distribute-certs.sh` and `distribute-secrets.sh` copy over SSH (Ubuntu zone, from `ctrl-2`) or locally from `ctrl-1` (RHEL zone). Every file lands `root:kafka`, `0640`, in directories `0750`:

| File | Content | Hosts |
| :--- | :--- | :--- |
| `/etc/kafka/certs/keystore.p12` | this host's key + cert | every VM |
| `/etc/kafka/certs/truststore.p12` | cluster CA | every VM |
| `/etc/kafka/certs/*.pem` | CA + exporter cert/key | `connect-2`, `connect-3` |
| `/etc/kafka/secrets/credentials.properties` | keystore/key/truststore passwords; `mongo.uri` only on Connect VMs | every VM |
| `/etc/kafka/secrets/service.env` | `AGENT_API_TOKEN` | `ctrl-1`, `ctrl-2`, `ctrl-4` |

**3. Consumed without ever writing plaintext into configs.**

| Service | How it reads secrets |
| :--- | :--- |
| Brokers, controllers, Connect workers | `FileConfigProvider`: `ssl.keystore.password=${file:/etc/kafka/secrets/credentials.properties:ssl.keystore.password}` |
| MongoDB connectors | `FileConfigProvider` in the connector JSON (`${file:...:mongo.uri}`). The URI never lands in the `connect-configs` topic, because only the placeholder is stored there. |
| REST Proxy | Same pattern as Swarm (`KAFKA_REST_CLIENT_SSL_KEYSTORE_PASSWORD=$$(cat /run/secrets/…)`): passwords are injected **at start time** and never written into `/etc`. `/etc/kafka/kafka-rest.properties.tmpl` holds placeholders. `ExecStartPre=/opt/kafka-ops/scripts/render-runtime-config.sh kafka-rest` fills them from `credentials.properties` into `/run/kafka-rest/kafka-rest.properties`. That's a tmpfs (`RuntimeDirectory=kafka-rest`, `0700`, `kafka` only), so it's wiped when the service stops or the VM reboots. `ExecStart` points at that runtime file. This works regardless of whether REST Proxy supports `${file:}`. |
| kafka_exporter | PEM key file path in its flags. No password, since the key is unencrypted and protected by file permissions. |
| `rhel-agent` and ops scripts | `AGENT_API_TOKEN` from `service.env` (systemd `EnvironmentFile=` for the agent; sourced by scripts) |

**4. Rotated** by editing the source file on `ctrl-2`, then `ops-sync.sh` → `distribute-secrets.sh` → rolling restart of the affected tier. Certs are rotated with `rotate-certs.sh`.

### 5.3 SELinux (RHEL rack)
SELinux stays **enforcing**. Since `/data` and `/opt/*` are non-default paths, `setup-node.sh` applies file contexts (`semanage fcontext` + `restorecon`) and opens ports with `semanage port` where needed. It must never call `setenforce 0`.

### 5.4 Service users and systemd units

**Users**, created by `setup-node.sh` with the **same UID/GID on every VM** (`kafka` = **1500**, confirmed free; set in `inventory.conf`) so file ownership stays consistent across hosts and archives. `setup-node.sh` stops if UID/GID 1500 already belongs to a different user or group, rather than silently picking another number:

| User | Where | Purpose |
| :--- | :--- | :--- |
| `kafka` (system user, `nologin`, home `/opt/kafka-home`) | every VM | runs every Kafka/ecosystem unit; owns `/data/kafka`, `/data/log/kafka`, `/data/prometheus` |
| `kafka-agent` (system user, `nologin`) | `ctrl-1` | runs `rhel-agent`; sudo limited to `systemctl start/stop/restart/is-active kafka-* ` plus SSH to Rack 1 hosts |
| `kafka-ops` (login user, SSH key) | `ctrl-2`, `ctrl-4` (+ authorized on Ubuntu VMs) | the account operators use to run the ops scripts; sudo limited to `systemctl … kafka-*`, `prometheus`, and the file distribution paths |

```bash
groupadd --system --gid 1500 kafka
useradd  --system --uid 1500 --gid kafka --home-dir /opt/kafka-home \
         --shell /sbin/nologin kafka
```

**Units** (`systemd/*.service`, installed to `/etc/systemd/system/`):

| Unit | VMs | `ExecStart` |
| :--- | :--- | :--- |
| `kafka-controller` | `ctrl-*` | `kafka-server-start /etc/kafka/controller.properties` |
| `kafka-broker` | `broker-*` | `kafka-server-start /etc/kafka/broker.properties` |
| `kafka-connect` | `connect-*` | `connect-distributed /etc/kafka/connect-distributed.properties` |
| `kafka-rest` | `connect-2/3` | `ExecStartPre=render-runtime-config.sh kafka-rest`, then `kafka-rest-start /run/kafka-rest/kafka-rest.properties` |
| `kafka-exporter` | `connect-2/3` | `/opt/kafka-exporter/kafka_exporter --kafka.server=… --tls.* …` |
| `prometheus` | `connect-2/3` | `/opt/prometheus/prometheus --config.file=… --storage.tsdb.*` |
| `rhel-agent` | `ctrl-1` | `/usr/bin/python3 /opt/kafka-ops/scripts/rhel-agent.py` |

Example, `kafka-broker.service`. The other Kafka units have the same shape:

```ini
[Unit]
Description=Apache Kafka broker
After=network-online.target
Wants=network-online.target
# give up after 3 failed starts in 10 min → a human investigates
StartLimitIntervalSec=600
StartLimitBurst=3

[Service]
Type=simple
User=kafka
Group=kafka
Environment=JAVA_HOME=/opt/java
Environment=LOG_DIR=/data/log/kafka
Environment=KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=/etc/kafka/log4j2.yaml
# per-host values rendered from inventory: KAFKA_HEAP_OPTS="-Xms6g -Xmx6g", KAFKA_JVM_PERFORMANCE_OPTS
EnvironmentFile=/etc/kafka/broker.env
ExecStart=/opt/confluent/bin/kafka-server-start /etc/kafka/broker.properties
Restart=on-failure
RestartSec=30
# controlled shutdown moves leadership away; give it time instead of SIGKILL
TimeoutStopSec=300
SuccessExitStatus=143
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
```

* Units are **enabled**, so a rebooted VM rejoins on its own. Brokers keep retrying until the controllers are reachable, so boot order across VMs doesn't matter. `cluster-start.sh` still enforces the order for planned starts.
* Services run in the **foreground** (no `-daemon`), so systemd tracks the real PID and nothing writes `*.out` files.
* On RHEL, the units and paths get SELinux contexts (§5.3).

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
9. **k8s and Grafana:** put the shared `k8s-clients` keystore/truststore into the k8s Secret(s) used by the apps and Kafbat, check from a pod that `:29092` is reachable on every broker (DNS is confirmed), and add both Prometheus datasources to Grafana.
10. **Cutover:** repoint clients and the log service to the new cluster, then decommission Swarm.

`setup-node.sh` installs OS packages from the bundle, creates the users and directories (§5.4), unpacks the binaries (and the exporter binary on `connect-2/3`), renders configs from `inventory.conf`, checks that every inventory host resolves in DNS (and stops if one doesn't), installs and enables units, configures journald limits, sets OS tuning (`vm.swappiness=1`, `nofile` ≥ 100000, `vm.max_map_count`), applies SELinux contexts on RHEL, and runs `kafka-storage format` on controllers and brokers. Formatting is skipped if `meta.properties` already exists.

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
3. If it's logs, check the Log4j2 caps (§10.1).

---

## 9. Offline Bundle (everything the air-gapped side needs)

**Rule:** nothing on the air-gapped side may reach the internet: no `dnf`/`apt` repos, no `pip`, no `curl` to public URLs, no image pulls from public registries. Everything is built once on an internet-connected **staging host**, checksummed, and carried over as one archive.

`build-offline-bundle.sh` produces `kafka-offline-bundle-<date>.tar.gz` plus `SHA256SUMS`. All versions and checksums are pinned in `offline-bundle/versions.env`, and the build fails if a download doesn't match its checksum.

### 9.1 Bundle contents

| Path in bundle | Content | Installed on |
| :--- | :--- | :--- |
| `binaries/confluent-8.x.y.tar.gz` | Confluent Platform (Kafka, Connect, REST Proxy, CLI tools) | all VMs (brokers/controllers use only the Kafka parts) |
| `binaries/OpenJDK21U-jdk_x64_linux_*.tar.gz` | JDK 21 (Temurin) | all VMs |
| `binaries/prometheus-<ver>.linux-amd64.tar.gz` | Prometheus | `connect-2`, `connect-3` |
| `connect-plugins/mongodb-kafka-connect-mongodb-<ver>.zip` | Official MongoDB connector (with all dependency jars) | `connect-*` |
| `os-packages/rhel9/*.rpm` | OS tools + all their dependencies (see 9.2) | RHEL VMs |
| `os-packages/ubuntu2204/*.deb` | OS tools + all their dependencies (see 9.2) | Ubuntu VMs |
| `repo/kafka-ops.tar.gz` | This Git repo (`git archive`) + a `git bundle` for the full history | `ctrl-1`, `ctrl-2`, `ctrl-4` |
| `grafana/dashboards/*.json` | Kafka exporter / lag dashboards, pre-downloaded | imported into the existing Grafana |
| `docs/` | Offline copies of the relevant docs (Kafka ops, Connect REST API, MongoDB connector config reference) | ops nodes |
| `SHA256SUMS`, `versions.env` | Integrity + exact versions | all |

**Not in the bundle, because they already exist on the air-gapped side:** the kafka_exporter v1.9.0 image (we extract the binary from it, §4.5), the Kafbat UI image, and anything k8s-side. `setup-node.sh` takes the exporter binary path as a parameter (`--exporter-bin`).

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
* Prometheus and Connect are configured with no update checks, telemetry or remote plugin loading. Confluent telemetry/metrics reporters are disabled (`confluent.telemetry.enabled=false`).
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
| `/etc/kafka/certs/`, `/etc/kafka/secrets/` | dirs `0750`, files `0640`, `root:kafka` (the service can read but not modify) |
| `/data/kafka` | Broker logs / controller metadata |
| `/data/prometheus` | Prometheus TSDB |
| `/data/log/kafka` | Service logs (see 10.1) |
| `/opt/kafka-home` | `kafka` user home (empty) |

### 10.1 Log rotation (no `logrotate` needed)

Kafka has two kinds of "logs", and each one handles its own cleanup:

| What | Where | Who cleans it |
| :--- | :--- | :--- |
| **Topic data** (Kafka "log segments") | `/data/kafka` | **Kafka itself**: `log.retention.hours` / `retention.bytes` delete old segments. This is retention, not rotation, and must **never** be touched with `logrotate` or `rm`. |
| **Application logs** (`server.log`, `controller.log`, `connect.log`, `kafka-rest.log`, `state-change.log`, …) | `/data/log/kafka` | **Log4j2**: our `log4j2.yaml` uses `RollingFile` with a size trigger (100 MB) and `DefaultRolloverStrategy max=10` + a `Delete` action, so each file is capped at about 1 GB. |
| **GC logs** | `/data/log/kafka/*-gc.log*` | **The JVM**: Kafka's start scripts already pass `-Xlog:gc*:file=…:filecount=10,filesize=100M`. |
| **stdout/stderr** of every unit (exporter, Prometheus, startup errors) | journald | **journald**: drop-in `/etc/systemd/journald.conf.d/kafka.conf` with `SystemMaxUse=1G`. |
| `rhel-agent` audit log | `/data/log/kafka/rhel-agent.log` | Python `RotatingFileHandler` (10 MB × 5). |

⚠️ The **stock** `log4j2.yaml` shipped with Kafka rolls files by **time and never deletes them**, which on a 100 GB disk eventually fills `/data`. That's why we ship our own. Worst case for logs is about 8–10 GB per VM.

Kafka 4.x reads only Log4j2 (`log4j2.yaml`); Log4j 1.x configs from 3.9 don't work.

---

## 11. Network / Firewall

| Port | From → To | Purpose |
| :--- | :--- | :--- |
| 9093 | controllers ↔ controllers, brokers → controllers | KRaft (mTLS) |
| 9092 | brokers ↔ brokers; Connect VMs, `ctrl-2`, `ctrl-4` → brokers | `INTERNAL`: inter-broker + VM-side clients (mTLS) |
| 29092 | **k8s** (apps, Kafbat UI) → brokers | `EXTERNAL`: k8s clients (mTLS) |
| 8083 | ops nodes, connect ↔ connect, **k8s (Kafbat)** → Connect VMs | Connect REST (HTTPS) |
| 8082 | **k8s (log service)** → `connect-2`, `connect-3` | REST Proxy (HTTPS) |
| 9308 | Prometheus → exporters (`connect-2`, `connect-3`) | Exporter metrics |
| 9090 | Grafana → `connect-2`, `connect-3` | Prometheus |
| 6000 | **`ctrl-2`, `ctrl-4` → `ctrl-1` only** | `rhel-agent` (HTTPS) |
| 22 | within the Ubuntu zone (ops nodes → all Ubuntu VMs, `ctrl-2` ↔ `ctrl-4`); within the RHEL zone (`ctrl-1` → RHEL VMs) | SSH |
| 27017 | Connect VMs → MongoDB | Connector traffic |

All 17 VMs must resolve each other by FQDN through **corporate DNS**, and the names must match the cert SANs. k8s pods must resolve the brokers' `external_host` names (§2.2).

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
│   ├── log4j2.yaml                # size-capped RollingFile + Delete
│   ├── journald-kafka.conf
│   ├── topics.conf
│   └── acls.conf
├── connectors/*.json
├── systemd/
│   ├── kafka-controller.service   kafka-broker.service   kafka-connect.service
│   ├── kafka-rest.service         kafka-exporter.service prometheus.service
│   ├── rhel-agent.service
│   └── *.env.tmpl                 # per-host heap/JVM opts
├── grafana/dashboards/            # dashboards shipped in the bundle
└── scripts/
    ├── lib/                       # common.sh (inventory parsing, ssh/agent dispatch, locking)
    ├── rhel-agent.py
    ├── build-offline-bundle.sh  setup-node.sh  distribute-certs.sh  distribute-secrets.sh
    ├── cluster-start.sh  cluster-shutdown.sh  cluster-restart.sh  rolling-restart.sh
    ├── health-check.sh  get-logs.sh  create-topics.sh  create-acls.sh
    ├── rotate-certs.sh  ops-sync.sh  render-runtime-config.sh
    └── connectors/ create-all.sh  restart-all.sh  delete-all.sh
```

---

## 13. Open Items & Risks

1. **Staging host:** needs internet, Docker/Podman (for the RHEL 9 / Ubuntu 22.04 package containers), and a RHEL subscription or UBI repos for the RPM downloads.
2. **Version check:** confirm the exact CP 8.x / Kafka 4.x release and the MongoDB connector version available for the bundle, and that they're compatible with JDK 21.
3. **No host or JMX metrics:** disk-full on the 100 GB disks is the most likely outage, and only `health-check.sh` catches it. Consider adding `node_exporter` later.
4. **100 GB per broker** is small. Every new topic needs a capacity check against §4.1.
5. **REST Proxy failover** is manual, and v2 consumer state is lost on failover (§4.4).
6. **`rhel-agent`** is custom code with control over Rack 1. It needs code review and a security sign-off.
7. **Static quorum:** changing the controller set later needs a planned migration to a dynamic quorum (KIP-853).
8. **CA passphrase holders:** name the operators who know the CA passphrase (the key itself lives encrypted on `ctrl-2`/`ctrl-4`, §5.1).
9. **Shared k8s client cert:** one principal for all k8s apps (§5.1). Accepted for now; revisit if per-app ACLs or auditing are needed.
