/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.proxy.testplugins.ClientTlsAwareLawyer;
import io.kroxylicious.proxy.testplugins.SaslPlainTermination;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
public class SaslPlainTerminatorIT extends BaseIT {

    private static void doAThing(KafkaCluster cluster,
                                 Topic topic,
                                 String mechanismName,
                                 String clientJaasCofig,
                                 @Nullable Consumer<Producer<String, String>> producerAction,
                                 @Nullable Consumer<ConsumerRecords<String, String>> consumerAction) {
        var clientSaslConfigs = Map.of(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, mechanismName,
                SaslConfigs.SASL_JAAS_CONFIG, clientJaasCofig);

        Map<String, Object> producerConfigs = new HashMap<>(Map.of(
                CLIENT_ID_CONFIG, "shouldTerminate-producer",
                DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
        producerConfigs.putAll(clientSaslConfigs);

        Map<String, Object> consumerConfigs = new HashMap<>(Map.of(CLIENT_ID_CONFIG, "shouldTerminate-consumer",
                GROUP_ID_CONFIG, "my-group-id",
                AUTO_OFFSET_RESET_CONFIG, "earliest"));
        consumerConfigs.putAll(clientSaslConfigs);

        NamedFilterDefinition saslTermination = new NamedFilterDefinitionBuilder(
                SaslPlainTermination.class.getName(),
                SaslPlainTermination.class.getName())
                .build();
        NamedFilterDefinition lawyer = new NamedFilterDefinitionBuilder(
                ClientTlsAwareLawyer.class.getName(),
                ClientTlsAwareLawyer.class.getName())
                .build();
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination, lawyer)
                .addToDefaultFilters(saslTermination.name(), lawyer.name());

        try (var tester = kroxyliciousTester(config)) {
            if (producerAction != null) {
                try (var producer = tester.producer(producerConfigs)) {
                    producerAction.accept(producer);
                }
            }
            if (consumerAction != null) {
                try (var consumer = tester.consumer(consumerConfigs)) {
                    consumer.subscribe(Set.of(topic.name()));
                    var records = consumer.poll(Duration.ofSeconds(10));
                    consumerAction.accept(records);
                }
            }
        }
    }

    /*
     * SaslPlainClient
     *
     * common tests:
     * for each api version:
     * * client not configured for sasl
     * -- abstract creates
     * -- concrete provides a proxy-cluster pair and a client configs
     * * client doesn't send a handshake, goes straight for authenticate
     * * client tries a disabled mechanism in handshake
     *
     * common reauth tests:
     * * client does not reauthenticate in time
     * * client does reauthenticate in time, but with the wrong password
     * * client does reauthenticate in time, with the right password
     *
     * Abstract class has the above tests, which are really about
     * This class sets up the cluster
     */
    @Test
    void shouldAuthenticateClientWithValidCreds(KafkaCluster cluster, Topic topic) {

        String mechanismName = "PLAIN";

        String clientJaasCofig = """
                org.apache.kafka.common.security.plain.PlainLoginModule required
                    username="alice"
                    password="alice-secret";
                """;

        doAThing(cluster, topic, mechanismName, clientJaasCofig,
                producer -> {
                    Assertions.assertThatCode(() -> {
                        producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")).get();
                        producer.flush();
                    }).doesNotThrowAnyException();
                },
                records -> {
                    assertThat(records).hasSize(1);
                    var recordHeaders = assertThat(records.records(topic.name()))
                            .as("topic %s records", topic.name())
                            .singleElement()
                            .asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                            .headers();
                    recordHeaders.firstHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_CLIENT_SASLPRINCIPAL_NAME).hasValueEqualTo("alice");
                });
    }

    @Test
    void shouldNotAuthenticateClientWithWrongPassword(KafkaCluster cluster, Topic topic) {

        String mechanismName = "PLAIN";

        String clientJaasCofig = """
                org.apache.kafka.common.security.plain.PlainLoginModule required
                    username="alice"
                    password="wrong-password";
                """;
        doAThing(cluster, topic, mechanismName, clientJaasCofig,
                producer -> {
                    assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value"))).failsWithin(5, TimeUnit.SECONDS)
                            .withThrowableOfType(ExecutionException.class)
                            .withCauseExactlyInstanceOf(SaslAuthenticationException.class);
                },
                null);
    }

    @Test
    void shouldNotAuthenticateClientWithUnknownUserName(KafkaCluster cluster, Topic topic) {

        String mechanismName = "PLAIN";

        String clientJaasCofig = """
                org.apache.kafka.common.security.plain.PlainLoginModule required
                    username="bob"
                    password="alice-secret";
                """;
        doAThing(cluster, topic, mechanismName, clientJaasCofig,
                producer -> {
                    assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value"))).failsWithin(5, TimeUnit.SECONDS)
                            .withThrowableOfType(ExecutionException.class)
                            .withCauseExactlyInstanceOf(SaslAuthenticationException.class);
                },
                null);
    }
}
