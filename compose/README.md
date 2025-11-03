## Purpose
This folder is intended to simplify the local deployment and manual testing of Kroxylicious especially in respect to non Java clients.

## Usage
`kafka-compose.yaml` starts a 3 node Kafka cluster and configures a single instance of Kroxylicious to load the `compose-proxy-config.yaml`.
The Kroxylicious image defaults to one built by the local maven build. 

To update the image quickly use a command such as 
```shell
mci -P dist -DskipTests -pl :kroxylicious-app -DskipDocs=true && podman image load -i $(pwd)/kroxylicious-app/target/kroxylicious-proxy.img.tar.gz
```

Note the container is not automatically restarted when the image or the config file is updated one has to do so manually e.g.
```shell
podman compose --file compose/kafka-compose.yaml up kroxylicious
```

## Accessing the Kroxylicious

The compose file makes kroxylicious available inside its own network via DNS as `kroxylicious:9192` it also makes it available from the host by exposing ports `9292-9196` with the associated config in the proxy configuration file. Which can be used to connect from the host via `localhost`

```shell
kafka-topics.sh --bootstrap-server localhost:9292 --create --if-not-exists --topic test.example
```