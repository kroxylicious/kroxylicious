# Manual testing: Envoy + PROXY protocol + Kroxylicious

This setup runs Envoy as a TCP proxy in front of Kroxylicious, adding
HAProxy PROXY protocol v2 headers to every connection.

## Architecture

```
Kafka client ──► Envoy (Docker, ports 59192-59195)
                   │ adds PROXY protocol v2 header
                   ▼
               Kroxylicious (host, ports 49192-49195)
                   │
                   ▼
               Kafka broker (target cluster)
```

## Port mapping

| Component      | Bootstrap | Broker 0 | Broker 1 | Broker 2 |
|----------------|-----------|----------|----------|----------|
| Envoy (client-facing) | 59192 | 59193 | 59194 | 59195 |
| Kroxylicious (bind)   | 49192 | 49193 | 49194 | 49195 |

**Note**: With `portIdentifiesNode`, bind port = advertised port. After the
bootstrap connection goes through Envoy (59192), subsequent broker connections
from the Kafka client use the addresses from the metadata response (localhost:49193+)
which go directly to Kroxylicious, bypassing Envoy. This is sufficient for
verifying PROXY protocol on the bootstrap path.

## Prerequisites

- Docker / Docker Compose
- A Kafka broker (update `targetCluster.bootstrapServers` in the config)
- Kroxylicious running from IntelliJ with `kroxylicious-proxy-config.yaml`

## Steps

1. Start Kafka broker
2. Start Kroxylicious from IntelliJ using `dev/envoy-proxy-protocol/kroxylicious-proxy-config.yaml`
3. Start Envoy:
   ```bash
   cd dev/envoy-proxy-protocol
   docker compose up -d
   ```
4. Connect a Kafka client through Envoy:
   ```bash
   kafka-topics --bootstrap-server localhost:59192 --list
   ```
5. Check Kroxylicious logs — you should see the PROXY protocol header being decoded
   with the original client source address.

## Cleanup

```bash
docker compose down
```
