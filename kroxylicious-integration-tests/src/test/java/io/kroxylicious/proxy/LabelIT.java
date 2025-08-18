/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedLabelSourceDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsBuilder;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.labels.Label;
import io.kroxylicious.proxy.labels.LabelledResource;
import io.kroxylicious.proxy.labels.source.simple.InlineLabelSourceFactory;
import io.kroxylicious.proxy.labels.source.simple.MtlsAwareInlineLabelSourceFactory;
import io.kroxylicious.proxy.testplugins.AbstractProduceHeaderInjectionFilter;
import io.kroxylicious.proxy.testplugins.LabelHeaderInjection;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class LabelIT extends AbstractTlsIT {
    @Test
    void shouldLabelTopicWithInlineLabel(KafkaCluster cluster, Topic topic, Topic topic2) throws Exception {

        ConfigurationBuilder proxy = KroxyliciousConfigUtils.proxy(cluster.getBootstrapServers(), KroxyliciousConfigUtils.DEFAULT_VIRTUAL_CLUSTER);
        InlineLabelSourceFactory.Config config = new InlineLabelSourceFactory.Config(
                Map.of(LabelledResource.TOPIC, new InlineLabelSourceFactory.ResourceLabels(Map.of(topic.name(), Set.of(new Label("is-a-topic", "true"))))));
        proxy.editFirstVirtualCluster().addToFilters("label").addToLabelSources(new NamedLabelSourceDefinition("inline", "InlineLabelSourceFactory", config))
                .endVirtualCluster();

        proxy.addToFilterDefinitions(new NamedFilterDefinition("label", LabelHeaderInjection.class.getSimpleName(),
                new LabelHeaderInjection.Config(Map.of(topic.name(), new LabelHeaderInjection.TopicLabels(
                        List.of("is-a-topic"))))));

        try (var tester = kroxyliciousTester(proxy);
                var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "shouldPassThroughRecordUnchanged", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester.consumer()) {
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "Hello, world!")).get();
            producer.send(new ProducerRecord<>(topic2.name(), "my-key", "Hello, world!")).get();
            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records.records(topic.name()))
                    .as("topic %s records", topic.name())
                    .singleElement()
                    .asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                    .headers().singleHeaderWithKey(AbstractProduceHeaderInjectionFilter.headerName(LabelHeaderInjection.class, "is-a-topic")).hasValueEqualTo("true");

            consumer.subscribe(Set.of(topic2.name()));
            var records2 = consumer.poll(Duration.ofSeconds(10));
            assertThat(records2.records(topic2.name()))
                    .as("topic %s records", topic.name())
                    .singleElement()
                    .asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                    .headers().hasSize(0);

        }
    }

    @Test
    void clientTlsContextMutualTls(KafkaCluster cluster,
                                   Admin admin) {
        var proxyKeystorePassword = downstreamCertificateGenerator.getPassword();
        String applePrefixTopic = "apple-a";
        String bananaPrefixTopic = "banana-a";
        CreateTopicsResult topics = admin.createTopics(List.of(new NewTopic(applePrefixTopic, 1, (short) 1), new NewTopic(bananaPrefixTopic, 1, (short) 1)));
        try {
            topics.all().get(5, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        Optional<Tls> gatewayTls;
        Objects.requireNonNull(proxyKeystorePassword, "proxyKeystorePassword is null");
        var proxyKeystoreLocation = downstreamCertificateGenerator.getKeyStoreLocation();
        var proxyKeystorePasswordProvider = constructPasswordProvider(InlinePassword.class, proxyKeystorePassword);
        // @formatter:off
        gatewayTls=Optional.of(new TlsBuilder()
                .withNewKeyStoreKey()
                .withStoreFile(proxyKeystoreLocation)
                .withStorePasswordProvider(proxyKeystorePasswordProvider)
                .endKeyStoreKey()
                .withNewTrustStoreTrust()
                .withNewServerOptionsTrust()
                .withClientAuth(TlsClientAuth.REQUIRED)
                .endServerOptionsTrust()
                .withStoreFile(proxyTrustStore.toAbsolutePath().toString())
                .withNewInlinePasswordStoreProvider(clientCertGenerator.getPassword())
                .endTrustStoreTrust()
                .build());
        // @formatter:on

        Map<String, Object> clientConfigs = Map.of(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, proxyKeystorePassword,
                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientCertGenerator.getKeyStoreLocation(),
                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientCertGenerator.getPassword());
        var bootstrapServers = cluster.getBootstrapServers();

        /**
         * VirtualCluster has an MTLS labelling source
         * principal: client
         *  TOPIC:
         *    - resourceNamePattern: apple-.*
         *      labels:
         *        is_allowed: true
         *    - resourceNamePattern: banana-.*
         *      labels:
         *        is_allowed: false
         *
         * (What if multiple LabelSources disagree on a label value? Do we make the API multi-valued, or fail fast?)
         */
        MtlsAwareInlineLabelSourceFactory.LabelPolicy appleLabelPolicy = new MtlsAwareInlineLabelSourceFactory.LabelPolicy(
                Map.of("apple-.*", Set.of(new Label("is_allowed", "true")), "banana-.*", Set.of(new Label("is_allowed", "false"))));
        MtlsAwareInlineLabelSourceFactory.ResourceLabels resourceLabels = new MtlsAwareInlineLabelSourceFactory.ResourceLabels(
                Map.of(LabelledResource.TOPIC, appleLabelPolicy));
        MtlsAwareInlineLabelSourceFactory.Config labelConfig = new MtlsAwareInlineLabelSourceFactory.Config(Map.of("client", resourceLabels));

        /**
         * Filter looks up the label for key is_allowed and adds it to the Produce Record as a header. The Filter is unaware
         * that the Principal is involved in deriving the label value this is not surfaced in the LabelService API.
         *
         * The framework is responsible for routing the authentication details to the Label Source.
         */
        LabelHeaderInjection.TopicLabels labelsToInject = new LabelHeaderInjection.TopicLabels(List.of("is_allowed"));
        // @formatter:off
        String demoCluster = "demo";
        var builder = new ConfigurationBuilder()
                .addNewFilterDefinition("clientConnection", LabelHeaderInjection.class.getName(), new LabelHeaderInjection.Config(Map.of(applePrefixTopic, labelsToInject, bananaPrefixTopic, labelsToInject)))
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName(demoCluster)
                        .addToFilters("clientConnection")
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withTls(gatewayTls)
                                .build())
                        .addToLabelSources(new NamedLabelSourceDefinition("mtlsAware", MtlsAwareInlineLabelSourceFactory.class.getName(), labelConfig))
                        .build());
        // @formatter:on

        String headerKey = AbstractProduceHeaderInjectionFilter.headerName(LabelHeaderInjection.class, "is_allowed");
        try (var tester = kroxyliciousTester(builder)) {
            assertProducedMessageHasHeaderAdded(tester, demoCluster, clientConfigs, applePrefixTopic, headerKey, "true");
            assertProducedMessageHasHeaderAdded(tester, demoCluster, clientConfigs, bananaPrefixTopic, headerKey, "false");
        }
    }

    private static void assertProducedMessageHasHeaderAdded(KroxyliciousTester tester, String demoCluster, Map<String, Object> clientConfigs, String topic,
                                                            String headerKey,
                                                            String expectedHeaderValue) {
        try (Producer<String, String> producer = tester.producer(demoCluster, clientConfigs)) {
            producer.send(new ProducerRecord<>(topic, "hello", "world"));
            producer.flush();
        }

        List<ConsumerRecord<String, String>> records;
        try (var consumer = tester.consumer(demoCluster, clientConfigs)) {
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.assign(Set.of(tp));
            consumer.seekToBeginning(Set.of(tp));
            do {
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(100));
                records = poll.records(tp);
            } while (records.isEmpty());
        }
        var recordHeaders = assertThat(records).singleElement().asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                .headers();
        recordHeaders.singleHeaderWithKey(headerKey)
                .hasValueEqualTo(expectedHeaderValue);
    }

}
