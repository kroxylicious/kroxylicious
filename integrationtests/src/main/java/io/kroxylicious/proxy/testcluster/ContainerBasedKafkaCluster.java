/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.testcluster;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.kroxylicious.proxy.testcluster.ClusterConfig.KafkaEndpoints;
import io.kroxylicious.proxy.testcluster.ClusterConfig.KafkaEndpoints.Endpoint;
import lombok.SneakyThrows;

/**
 * Provides an easy way to launch a Kafka cluster with multiple brokers in a container
 */
public class ContainerBasedKafkaCluster implements Startable, Cluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerBasedKafkaCluster.class);
    public static final int KAFKA_PORT = 9093;
    public static final int ZOOKEEPER_PORT = 2181;
    private static final DockerImageName DEFAULT_KAFKA_IMAGE = DockerImageName.parse("bitnami/kafka:latest");
    private static final DockerImageName DEFAULT_ZOOKEEPER_IMAGE = DockerImageName.parse("bitnami/zookeeper:latest");
    private static final int READY_TIMEOUT_SECONDS = 120;
    private final DockerImageName kafkaImage;
    private final DockerImageName zookeeperImage;
    private final ClusterConfig clusterConfig;
    private final Network network = Network.newNetwork();
    private final GenericContainer<?> zookeeper;
    private final Collection<KafkaContainer> brokers;

    static {
        if (!System.getenv().containsValue("TESTCONTAINERS_RYUK_DISABLED")) {
            LOGGER.warn("As per https://github.com/containers/podman/issues/7927#issuecomment-731525556 if using podman, set env var TESTCONTAINERS_RYUK_DISABLED=true");
        }
    }

    public ContainerBasedKafkaCluster(ClusterConfig clusterConfig) {
        this(null, null, clusterConfig);
    }

    public ContainerBasedKafkaCluster(DockerImageName kafkaImage, DockerImageName zookeeperImage, ClusterConfig clusterConfig) {
        this.kafkaImage = Optional.ofNullable(kafkaImage).orElse(DEFAULT_KAFKA_IMAGE);
        this.zookeeperImage = Optional.ofNullable(zookeeperImage).orElse(DEFAULT_ZOOKEEPER_IMAGE);
        this.clusterConfig = clusterConfig;

        if (this.clusterConfig.isKraftMode()) {
            this.zookeeper = null;
        }
        else {
            var logFile = MountableFile.forClasspathResource("zookeeper_logback.xml", 0644);
            this.zookeeper = new GenericContainer<>(this.zookeeperImage)
                    .withNetwork(network)
                    .withNetworkAliases("zookeeper")
                    .withEnv("ALLOW_ANONYMOUS_LOGIN", "yes")
                    .withEnv("ZOO_PORT_NUMBER", String.valueOf(ZOOKEEPER_PORT))
                    .withCopyToContainer(logFile, "/tmp/zookeeper_logback.xml");
        }

        Supplier<KafkaEndpoints> endPointConfigSupplier = () -> new KafkaEndpoints() {
            final List<Integer> ports = Utils.preAllocateListeningPorts(clusterConfig.getBrokersNum()).collect(Collectors.toList());

            @Override
            public EndpointPair getClientEndpoint(int brokerNum) {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", 9093)).connect(new Endpoint("localhost", ports.get(brokerNum))).build();
            }

            @Override
            public EndpointPair getInterBrokerEndpoint(int brokerNum) {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", 9092)).connect(new Endpoint(String.format("broker-%d", brokerNum), 9092)).build();
            }

            @Override
            public EndpointPair getControllerEndpoint(int brokerNum) {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", 9091)).connect(new Endpoint(String.format("broker-%d", brokerNum), 9091)).build();
            }
        };
        Supplier<Endpoint> zookeeperEndpointSupplier = () -> new Endpoint("zookeeper", ContainerBasedKafkaCluster.ZOOKEEPER_PORT);
        this.brokers = clusterConfig.getBrokerConfigs(endPointConfigSupplier, zookeeperEndpointSupplier).map(holder -> {
            String netAlias = "broker-" + holder.getBrokerNum();
            var kafkaJaasConf = MountableFile.forClasspathResource("kafka_jaas.conf", 0644);
            KafkaContainer kafkaContainer = new KafkaContainer(this.kafkaImage)
                    .withNetwork(this.network)
                    .withNetworkAliases(netAlias)
                    .withEnv("ALLOW_PLAINTEXT_LISTENER", "yes")
                    .withEnv("BITNAMI_DEBUG", "true")
                    .withEnv("XSHELLOPTS", "xtrace")
                    .withCopyToContainer(Transferable.of(propertiesToBytes(holder.getProperties()), 0666), "/opt/bitnami/kafka/config/server.properties")
                    .withCopyToContainer(kafkaJaasConf, "/opt/bitnami/kafka/config/kafka_jaas.conf")
                    .withStartupTimeout(Duration.ofMinutes(2));

            kafkaContainer.addFixedExposedPort(holder.getExternalPort(), KAFKA_PORT);

            // The Bitnami scripts requires that some properties are set as env vars.
            kafkaContainer.withEnv(serverPropertiesToBitnamiEnvVar(holder.getProperties()));

            if (this.clusterConfig.isKraftMode()) {
                return kafkaContainer
                        .withEnv("KAFKA_ENABLE_KRAFT", "yes")
                        .withEnv("KAFKA_KRAFT_CLUSTER_ID", holder.getKafkaKraftClusterId());
            }
            else {
                return kafkaContainer.dependsOn(this.zookeeper);
            }
        }).collect(Collectors.toList());
    }

    private byte[] propertiesToBytes(Properties properties) {
        try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
            properties.store(byteArrayOutputStream, "server.properties");
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, String> serverPropertiesToBitnamiEnvVar(Properties serverProperties) {
        Map<String, String> envVars = new HashMap<>();
        serverProperties.entrySet().forEach(p -> envVars.put("KAFKA_CFG_" + p.getKey().toString().toUpperCase().replace('.', '_'), p.getValue().toString()));
        return envVars;
    }

    @Override
    public String getBootstrapServers() {
        return brokers.stream()
                .map(b -> String.format("localhost:%d", b.getMappedPort(KAFKA_PORT)))
                .collect(Collectors.joining(","));
    }

    private Stream<GenericContainer<?>> allContainers() {
        return Stream.concat(
                this.brokers.stream(),
                Stream.ofNullable(this.zookeeper));
    }

    @Override
    @SneakyThrows
    public void start() {
        try {
            if (zookeeper != null) {
                zookeeper.start();
                Unreliables.retryUntilTrue(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS, () -> {
                    Container.ExecResult result = this.zookeeper.execInContainer(
                            "sh", "-c",
                            String.format("_JAVA_OPTIONS=-Dlogback.configurationFile=/tmp/zookeeper_logback.xml zkCli.sh -server zookeeper:%s whoami", ZOOKEEPER_PORT));
                    LOGGER.debug("zookeeper running {} / {} / exit {}", result.getStdout(), result.getStderr(), result.getExitCode());
                    return result.getExitCode() == 0;
                });
            }
            Startables.deepStart(brokers.stream()).get(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            stop();
            throw new RuntimeException("startup failed or timed out", e);
        }

        if (clusterConfig.isKraftMode()) {
            Unreliables.retryUntilTrue(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS, () -> {
                Container.ExecResult result = this.brokers.iterator().next().execInContainer(
                        "sh", "-c",
                        "kafka-metadata-shell.sh --snapshot /bitnami/kafka/data/__cluster_metadata-0/*.log ls /brokers");
                LOGGER.debug("kraft cluster ready probe {} / {} / exit {}", result.getStdout(), result.getStderr(), result.getExitCode());
                Optional<String> brokers = Optional.ofNullable(result.getStdout());
                return brokers.map(b -> Arrays.stream(b.split(System.lineSeparator())).count()).map(count -> count.intValue() == this.clusterConfig.getBrokersNum())
                        .orElse(false);
            });
        }
        else {
            Unreliables.retryUntilTrue(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS, () -> {
                Container.ExecResult result = this.zookeeper.execInContainer(
                        "sh", "-c",
                        String.format("_JAVA_OPTIONS=-Dlogback.configurationFile=/tmp/zookeeper_logback.xml zkCli.sh -server zookeeper:%s ls /brokers/ids | tail -n 1",
                                ZOOKEEPER_PORT));
                LOGGER.debug("zookeeper cluster ready probe {} / {} / exit {}", result.getStdout(), result.getStderr(), result.getExitCode());
                String brokers = result.getStdout();

                return brokers != null && brokers.split(",").length == this.clusterConfig.getBrokersNum();
            });
        }
    }

    @Override
    public void stop() {
        allContainers().parallel().forEach(GenericContainer::stop);
    }

    @Override
    public Map<String, Object> getConnectConfigForCluster() {
        return clusterConfig.getConnectConfigForCluster(getBootstrapServers());
    }

    // In kraft mode, currently "Advertised listeners cannot be altered when using a Raft-based metadata quorum", so we
    // need to know the listening port before we start the kafka container. For this reason, we need this override
    // to expose addFixedExposedPort to for use.
    public static class KafkaContainer extends GenericContainer<KafkaContainer> {
        public KafkaContainer(final DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        protected void addFixedExposedPort(int hostPort, int containerPort) {
            super.addFixedExposedPort(hostPort, containerPort);
        }
    }
}
