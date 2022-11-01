/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.testkafkacluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.TestInfo;

import io.kroxylicious.proxy.testkafkacluster.KafkaClusterConfig.KafkaEndpoints.Endpoint;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

@Builder(toBuilder = true)
@Getter
@ToString
public class KafkaClusterConfig {

    private TestInfo testInfo;

    /**
     * specifies the cluster execution mode.
     */
    private final KafkaClusterExecutionMode execMode;
    /**
     * if true, the cluster will be brought up in Kraft-mode
     */
    private final Boolean kraftMode;

    /**
     * name of SASL mechanism to be configured on kafka for the external listener, if null, anonymous communication
     * will be used.
     */
    private final String saslMechanism;
    @Builder.Default
    private Integer brokersNum = 1;

    @Builder.Default
    private Integer kraftControllers = 1;

    private final String kafkaKraftClusterId = Uuid.randomUuid().toString();
    /**
     * The users and passwords to be configured into the server's JAAS configuration used for the external listener.
     */
    @Singular
    private final Map<String, String> users;

    public Stream<ConfigHolder> getBrokerConfigs(Supplier<KafkaEndpoints> endPointConfigSupplier, Supplier<Endpoint> zookeeperEndpointSupplier) {
        List<ConfigHolder> properties = new ArrayList<>();
        KafkaEndpoints kafkaEndpoints = endPointConfigSupplier.get();
        for (int brokerNum = 0; brokerNum < brokersNum; brokerNum++) {
            Properties server = new Properties();

            server.put("broker.id", Integer.toString(brokerNum));

            var interBrokerEndpoint = kafkaEndpoints.getInterBrokerEndpoint(brokerNum);
            var clientEndpoint = kafkaEndpoints.getClientEndpoint(brokerNum);

            // - EXTERNAL: used for communications to/from consumers/producers
            // - INTERNAL: used for inter-broker communications (always no auth)
            // - CONTROLLER: used for inter-broker controller communications (kraft - always no auth)

            var externalListenerTransport = saslMechanism == null ? "PLAINTEXT" : "SASL_PLAINTEXT";

            var protocolMap = new TreeMap<>();
            var listeners = new TreeMap<>();
            var advertisedListeners = new TreeMap<>();
            protocolMap.put("EXTERNAL", externalListenerTransport);
            listeners.put("EXTERNAL", clientEndpoint.getBind().toString());
            advertisedListeners.put("EXTERNAL", clientEndpoint.getConnect().toString());

            protocolMap.put("INTERNAL", "PLAINTEXT");
            listeners.put("INTERNAL", interBrokerEndpoint.getBind().toString());
            advertisedListeners.put("INTERNAL", interBrokerEndpoint.getConnect().toString());
            server.put("inter.broker.listener.name", "INTERNAL");

            if (isKraftMode()) {
                server.put("node.id", Integer.toString(brokerNum)); // Required by Kafka 3.3 onwards.

                var controllerEndpoint = kafkaEndpoints.getControllerEndpoint(brokerNum);
                var quorumVoters = IntStream.range(0, kraftControllers)
                        .mapToObj(b -> String.format("%d@%s", b, kafkaEndpoints.getControllerEndpoint(b).getConnect().toString())).collect(Collectors.joining(","));
                server.put("controller.quorum.voters", quorumVoters);
                server.put("controller.listener.names", "CONTROLLER");
                protocolMap.put("CONTROLLER", "PLAINTEXT");

                if (brokerNum == 0) {
                    server.put("process.roles", "broker,controller");

                    listeners.put("CONTROLLER", controllerEndpoint.getBind().toString());
                }
                else {
                    server.put("process.roles", "broker");
                }
            }
            else {
                server.put("zookeeper.connect", String.format("%s:%d", zookeeperEndpointSupplier.get().getHost(), zookeeperEndpointSupplier.get().getPort()));
                server.put("zookeeper.sasl.enabled", "false");
                server.put("zookeeper.connection.timeout.ms", Long.toString(60000));
            }

            server.put("listener.security.protocol.map", protocolMap.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
            server.put("listeners", listeners.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
            server.put("advertised.listeners", advertisedListeners.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(",")));
            server.put("early.start.listeners", advertisedListeners.keySet().stream().map(Object::toString).collect(Collectors.joining(",")));

            if (saslMechanism != null) {
                server.put("sasl.enabled.mechanisms", saslMechanism);

                var saslPairs = new StringBuilder();

                Optional.of(users).orElse(Map.of()).forEach((key, value) -> {
                    saslPairs.append(String.format("user_%s", key));
                    saslPairs.append("=");
                    saslPairs.append(value);
                    saslPairs.append(" ");
                });

                // TODO support other than PLAIN
                String plainModuleConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required %s;", saslPairs);
                server.put(String.format("listener.name.%s.plain.sasl.jaas.config", "EXTERNAL".toLowerCase()), plainModuleConfig);
            }

            server.put("offsets.topic.replication.factor", Integer.toString(1));
            // 1 partition for the __consumer_offsets_ topic should be enough
            server.put("offsets.topic.num.partitions", Integer.toString(1));
            // Disable delay during every re-balance
            server.put("group.initial.rebalance.delay.ms", Integer.toString(0));

            properties.add(new ConfigHolder(server, clientEndpoint.getConnect().getPort(),
                    String.format("%s:%d", clientEndpoint.getConnect().getHost(), clientEndpoint.getConnect().getPort()), brokerNum, kafkaKraftClusterId));
        }

        return properties.stream();
    }

    protected Map<String, Object> getConnectConfigForCluster(String bootstrapServers) {
        Map<String, Object> kafkaConfig = new HashMap<>();
        String saslMechanism = getSaslMechanism();
        if (saslMechanism != null) {
            kafkaConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            kafkaConfig.put(SaslConfigs.SASL_MECHANISM, saslMechanism);

            Map<String, String> users = getUsers();
            if ("PLAIN".equals(saslMechanism) && !users.isEmpty()) {
                var first = users.entrySet().iterator().next();
                kafkaConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
                        String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                                first.getKey(), first.getValue()));
            }
            else {
                throw new IllegalStateException(String.format("unsupported SASL mechanism %s", saslMechanism));
            }
        }

        kafkaConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return kafkaConfig;
    }

    public boolean isKraftMode() {
        return this.getKraftMode() == null || this.getKraftMode();
    }

    @Builder
    @Getter
    public static class ConfigHolder {
        private final Properties properties;
        private final Integer externalPort;
        private final String endpoint;
        private final int brokerNum;
        private final String kafkaKraftClusterId;
    }

    protected interface KafkaEndpoints {

        @Builder
        @Getter
        class EndpointPair {
            private final Endpoint bind;
            private final Endpoint connect;
        }

        @Builder
        @Getter
        class Endpoint {
            private final String host;
            private final int port;

            @Override
            public String toString() {
                return String.format("//%s:%d", host, port);
            }
        }

        EndpointPair getInterBrokerEndpoint(int brokerId);

        EndpointPair getControllerEndpoint(int brokerId);

        EndpointPair getClientEndpoint(int brokerId);
    }
}
