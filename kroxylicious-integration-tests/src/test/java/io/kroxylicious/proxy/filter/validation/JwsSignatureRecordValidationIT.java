/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.jws.JwsTestUtils.ECDSA_VERIFY_JWKS;
import static io.kroxylicious.test.jws.JwsTestUtils.JWS_HEADER_NAME;
import static io.kroxylicious.test.jws.JwsTestUtils.RSA_AND_ECDSA_VERIFY_JWKS;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_PAYLOAD;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_MISSING_ECDSA_JWK;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_RSA_JWK;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_RSA_JWK_PAYLOAD;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
public class JwsSignatureRecordValidationIT extends RecordValidationBaseIT {
    // Copied from JwsSignatureBytebufValidator
    private static final String DEFAULT_ERROR_MESSAGE = "JWS Signature could not be successfully verified";

    private static final byte[] INVALID_JWS = "This is a non JWS value".getBytes(StandardCharsets.UTF_8);
    private static final String RANDOM_STRING = "random string";
    private static final String EMPTY_STRING = "";

    @Test
    void validJwsProduceAccepted(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, false, true);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, VALID_JWS_USING_ECDSA_JWK_PAYLOAD, List.of(new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK))));
            assertThatFutureSucceeds(result);

            var records = consumeAll(tester, topic);
            assertThat(records)
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(VALID_JWS_USING_ECDSA_JWK_PAYLOAD);

            assertThat(records)
                    .singleElement()
                    .satisfies(record -> hasJwsHeaderWithValue(record, VALID_JWS_USING_ECDSA_JWK));
        }
    }

    @Test
    void missingJwsHeaderProduceAcceptedWhenConfigured(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, false, false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, VALID_JWS_USING_ECDSA_JWK_PAYLOAD, List.of()));
            assertThatFutureSucceeds(result);

            var records = consumeAll(tester, topic);
            assertThat(records)
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(VALID_JWS_USING_ECDSA_JWK_PAYLOAD);
        }
    }

    @Test
    void validJwsSignedWithJwkFromMultiKeyJwksProduceAccepted(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, true, false, true);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, VALID_JWS_USING_RSA_JWK_PAYLOAD, List.of(new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_RSA_JWK))));
            assertThatFutureSucceeds(result);

            var records = consumeAll(tester, topic);
            assertThat(records)
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(VALID_JWS_USING_RSA_JWK_PAYLOAD);

            assertThat(records)
                    .singleElement()
                    .satisfies(record -> hasJwsHeaderWithValue(record, VALID_JWS_USING_RSA_JWK));
        }
    }

    @Test
    void validDetachedContentJwsProduceAccepted(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, true, true);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD,
                    List.of(new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED))));
            assertThatFutureSucceeds(result);

            var records = consumeAll(tester, topic);
            assertThat(records)
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD);

            assertThat(records)
                    .singleElement()
                    .satisfies(record -> hasJwsHeaderWithValue(record, VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED));
        }
    }

    @Test
    void invalidJwsProduceRejected(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, false, true);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, RANDOM_STRING, List.of(new RecordHeader(JWS_HEADER_NAME, INVALID_JWS))));
            assertThatFutureFails(result, InvalidRecordException.class,
                    DEFAULT_ERROR_MESSAGE + ": Jose4j threw an exception: A JWS Compact Serialization must have exactly 3 parts");
        }
    }

    @Test
    void invalidJwsHeaderProduceRejectedWhenConfiguredToNotFailOnMissing(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, false, false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, RANDOM_STRING, List.of(new RecordHeader(JWS_HEADER_NAME, INVALID_JWS))));
            assertThatFutureFails(result, InvalidRecordException.class,
                    DEFAULT_ERROR_MESSAGE + ": Jose4j threw an exception: A JWS Compact Serialization must have exactly 3 parts");
        }
    }

    @Test
    void jwsSignedWithMissingJwkProduceRejected(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, false, true);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, RANDOM_STRING, List.of(new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_MISSING_ECDSA_JWK))));
            assertThatFutureFails(result, InvalidRecordException.class,
                    DEFAULT_ERROR_MESSAGE + ": Jose4j threw an exception: Could not select a valid JWK that matches the algorithm constraints");
        }
    }

    @Test
    void jwsSignedWithRsaJwkProduceRejectedDueToEcdsaConstraints(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, false, true);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, VALID_JWS_USING_RSA_JWK_PAYLOAD, List.of(new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_RSA_JWK))));
            assertThatFutureFails(result, InvalidRecordException.class,
                    DEFAULT_ERROR_MESSAGE + ": Jose4j threw an exception: Could not select a valid JWK that matches the algorithm constraints");
        }
    }

    @Test
    void invalidDetachedContentJwsProduceRejected(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, true, true);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, RANDOM_STRING, List.of(new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED))));
            assertThatFutureFails(result, InvalidRecordException.class, DEFAULT_ERROR_MESSAGE + ": JWS Signature was invalid");
        }
    }

    private static void hasJwsHeaderWithValue(ConsumerRecord<String, String> record, byte[] value) {
        assertThat(record.headers()).anySatisfy(header -> {
            assertThat(header.key()).isEqualTo(JWS_HEADER_NAME);
            assertThat(header.value()).isEqualTo(value);
        });
    }

    private static ConfigurationBuilder createFilterDef(KafkaCluster cluster, Topic topic, boolean multiKeyJwks, boolean contentDetached,
                                                        boolean requireJwsRecordHeader) {
        String className = RecordValidation.class.getName();

        Map<String, Object> jwsConfig;

        if (multiKeyJwks) {
            jwsConfig = Map.of("trustedJsonWebKeySet", RSA_AND_ECDSA_VERIFY_JWKS.toJson(), "algorithms", Map.of("allowed", List.of("ES256", "RS256")),
                    "recordHeader", Map.of("key", JWS_HEADER_NAME, "required", requireJwsRecordHeader),
                    "content", Map.of("detached", contentDetached));
        }
        else {
            jwsConfig = Map.of("trustedJsonWebKeySet", ECDSA_VERIFY_JWKS.toJson(), "algorithms", Map.of("allowed", List.of("ES256")), "recordHeader",
                    Map.of("key", JWS_HEADER_NAME, "required", requireJwsRecordHeader),
                    "content", Map.of("detached", contentDetached));
        }

        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className).withConfig("rules",
                List.of(Map.of("topicNames", List.of(topic.name()), "valueRule",
                        Map.of("jwsSignatureValidation", jwsConfig))))
                .build();

        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }
}
