/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.kafkacluster;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
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

import org.junit.jupiter.api.TestInfo;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectContainerResponse;

import lombok.SneakyThrows;

/**
 * Provides an easy way to launch a Kafka cluster with multiple brokers in a container
 */
public class ContainerBasedKafkaCluster implements Startable, KafkaCluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerBasedKafkaCluster.class);
    public static final int KAFKA_PORT = 9093;
    public static final int ZOOKEEPER_PORT = 2181;

    // FIXME: uses container image built from https://github.com/ozangunalp/kafka-native/pull/5
    private static final DockerImageName DEFAULT_KAFKA_IMAGE = DockerImageName.parse("quay.io/k_wall/kafka-native:1.0.0-SNAPSHOT");

    // FIXME: uses container image built from https://github.com/k-wall/zookeeper-native, move the repo to a permanent location.
    private static final DockerImageName DEFAULT_ZOOKEEPER_IMAGE = DockerImageName.parse("quay.io/k_wall/zookeeper-native:1.0.0-SNAPSHOT");
    private static final int READY_TIMEOUT_SECONDS = 120;
    private static final String KAFKA_CLUSTER_READY_FLAG = "/tmp/kafka_cluster_ready";
    private static final String ZOOKEEPER_READY_FLAG = "/tmp/kafka_zookeeper_ready";
    private final DockerImageName kafkaImage;
    private final DockerImageName zookeeperImage;
    private final KafkaClusterConfig clusterConfig;
    private final Network network = Network.newNetwork();
    private final ZookeeperContainer zookeeper;
    private final Collection<KafkaContainer> brokers;

    static {
        if (!System.getenv().containsKey("TESTCONTAINERS_RYUK_DISABLED")) {
            LOGGER.warn("As per https://github.com/containers/podman/issues/7927#issuecomment-731525556 if using podman, set env var TESTCONTAINERS_RYUK_DISABLED=true");
        }
    }

    public ContainerBasedKafkaCluster(KafkaClusterConfig clusterConfig) {
        this(null, null, clusterConfig);
    }

    public ContainerBasedKafkaCluster(DockerImageName kafkaImage, DockerImageName zookeeperImage, KafkaClusterConfig clusterConfig) {
        this.kafkaImage = Optional.ofNullable(kafkaImage).orElse(DEFAULT_KAFKA_IMAGE);
        this.zookeeperImage = Optional.ofNullable(zookeeperImage).orElse(DEFAULT_ZOOKEEPER_IMAGE);
        this.clusterConfig = clusterConfig;

        var name = Optional.ofNullable(clusterConfig.getTestInfo())
                .map(TestInfo::getDisplayName)
                .map(s -> s.replaceFirst("\\(\\)$", ""))
                .map(s -> String.format("%s.%s", s, OffsetDateTime.now()))
                .orElse(null);

        if (this.clusterConfig.isKraftMode()) {
            this.zookeeper = null;
        }
        else {
            this.zookeeper = new ZookeeperContainer(this.zookeeperImage)
                    .withName(name)
                    .withNetwork(network)
                    .withEnv("SERVER_ZOOKEEPER_READY_FLAG_FILE", ZOOKEEPER_READY_FLAG)
                    // .withEnv("QUARKUS_LOG_LEVEL", "DEBUG") // Enables org.apache.zookeeper logging too
                    .withNetworkAliases("zookeeper");
        }

        Supplier<KafkaClusterConfig.KafkaEndpoints> endPointConfigSupplier = () -> new KafkaClusterConfig.KafkaEndpoints() {
            final List<Integer> ports = Utils.preAllocateListeningPorts(clusterConfig.getBrokersNum()).collect(Collectors.toList());

            @Override
            public EndpointPair getClientEndpoint(int brokerId) {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", 9093)).connect(new Endpoint("localhost", ports.get(brokerId))).build();
            }

            @Override
            public EndpointPair getInterBrokerEndpoint(int brokerId) {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", 9092)).connect(new Endpoint(String.format("broker-%d", brokerId), 9092)).build();
            }

            @Override
            public EndpointPair getControllerEndpoint(int brokerId) {
                return EndpointPair.builder().bind(new Endpoint("0.0.0.0", 9091)).connect(new Endpoint(String.format("broker-%d", brokerId), 9091)).build();
            }
        };
        Supplier<KafkaClusterConfig.KafkaEndpoints.Endpoint> zookeeperEndpointSupplier = () -> new KafkaClusterConfig.KafkaEndpoints.Endpoint("zookeeper", ContainerBasedKafkaCluster.ZOOKEEPER_PORT);
        this.brokers = clusterConfig.getBrokerConfigs(endPointConfigSupplier, zookeeperEndpointSupplier).map(holder -> {
            String netAlias = "broker-" + holder.getBrokerNum();
            KafkaContainer kafkaContainer = new KafkaContainer(this.kafkaImage)
                    .withName(name)
                    .withNetwork(this.network)
                    .withNetworkAliases(netAlias)
                    // .withEnv("QUARKUS_LOG_LEVEL", "DEBUG") // Enables org.apache.kafka logging too
                    .withEnv("SERVER_PROPERTIES_FILE", "/cnf/server.properties")
                    .withEnv("SERVER_CLUSTER_ID", holder.getKafkaKraftClusterId())
                    .withEnv("SERVER_CLUSTER_READY_FLAG_FILE", KAFKA_CLUSTER_READY_FLAG)
                    .withEnv("SERVER_CLUSTER_READY_NUM_BROKERS", clusterConfig.getBrokersNum().toString())
                    .withCopyToContainer(Transferable.of(propertiesToBytes(holder.getProperties()), 0644), "/cnf/server.properties")
                    .withStartupTimeout(Duration.ofMinutes(2));

            kafkaContainer.addFixedExposedPort(holder.getExternalPort(), KAFKA_PORT);

            if (!this.clusterConfig.isKraftMode()) {
                kafkaContainer.dependsOn(this.zookeeper);
            }
            return kafkaContainer;
        }).collect(Collectors.toList());
    }

    private byte[] propertiesToBytes(Properties properties) {
        try (var byteArrayOutputStream = new ByteArrayOutputStream()) {
            properties.store(byteArrayOutputStream, "server.properties");
            return byteArrayOutputStream.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
                awaitContainerReadyFlagFile(zookeeper, ZOOKEEPER_READY_FLAG);
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

        awaitContainerReadyFlagFile(this.brokers.iterator().next(), KAFKA_CLUSTER_READY_FLAG);
    }

    @Override
    public void close() {
        this.stop();
    }

    private void awaitContainerReadyFlagFile(GenericContainer<?> container, String kafkaClusterReadyFlag) {
        Unreliables.retryUntilTrue(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS, () -> {
            container.execInContainer(
                    "sh", "-c",
                    String.format("while [ ! -f %s ]; do sleep .1; done", kafkaClusterReadyFlag));
            LOGGER.info("Container {} ready", container.getDockerImageName());
            return true;
        });
    }

    @Override
    public void stop() {
        allContainers().parallel().forEach(GenericContainer::stop);
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration() {
        return clusterConfig.getConnectConfigForCluster(getBootstrapServers());
    }

    // In kraft mode, currently "Advertised listeners cannot be altered when using a Raft-based metadata quorum", so we
    // need to know the listening port before we start the kafka container. For this reason, we need this override
    // to expose addFixedExposedPort to for use.
    public static class KafkaContainer extends LoggingGenericContainer<KafkaContainer> {

        public KafkaContainer(final DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        protected void addFixedExposedPort(int hostPort, int containerPort) {
            super.addFixedExposedPort(hostPort, containerPort);
        }

    }

    public static class ZookeeperContainer extends LoggingGenericContainer<ZookeeperContainer> {
        public ZookeeperContainer(DockerImageName zookeeperImage) {
            super(zookeeperImage);
        }
    }

    public static class LoggingGenericContainer<C extends GenericContainer<C>>
            extends GenericContainer<C> {
        private static final String CONTAINER_LOGS_DIR = "container.logs.dir";
        private String name;

        public LoggingGenericContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo) {
            super.containerIsStarting(containerInfo);

            Optional.ofNullable(System.getProperty(CONTAINER_LOGS_DIR)).ifPresent(logDir -> {
                var target = Path.of(logDir);
                if (name != null) {
                    target = target.resolve(name);
                }
                target = target.resolve(String.format("%s.%s.%s", getContainerName().replaceFirst(File.separator, ""), getContainerId(), "log"));
                target.getParent().toFile().mkdirs();
                try {
                    var writer = new FileWriter(target.toFile());
                    super.followOutput(outputFrame -> {
                        try {
                            if (outputFrame.equals(OutputFrame.END)) {
                                writer.close();
                            }
                            else {
                                writer.write(outputFrame.getUtf8String());
                            }
                        }
                        catch (IOException e) {
                            // ignore
                        }
                    });
                }
                catch (IOException e) {
                    logger().warn("Failed to create container log file: {}", target);
                }
            });

        }

        public LoggingGenericContainer<C> withName(String name) {
            this.name = name;
            return this;
        }
    }

}
