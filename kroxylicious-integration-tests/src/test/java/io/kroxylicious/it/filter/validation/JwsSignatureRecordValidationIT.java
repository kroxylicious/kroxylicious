/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.HeaderParameterNames;
import org.jose4j.lang.JoseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
public class JwsSignatureRecordValidationIT extends RecordValidationBaseIT {
    private static final String JWS_HEADER_NAME = "jws";

    // Copied from JwsSignatureBytebufValidatorTest
    private static final String ECDSA_SIGN_JWK_JSON = "{" +
            "\"kty\": \"EC\"," +
            "\"kid\": \"ECDSA JWK\"," +
            "\"key_ops\": [ \"sign\" ]," +
            "\"alg\": \"ES256\"," +
            "\"d\": \"Tk7qzHNnSBMioAU7NwZ9JugFWmWbUCyzeBRjVcTp_so\"," +
            "\"x\": \"qqeGjWmYZU5M5bBrRw1zqZcbPunoFVxsfaa9JdA0R5I\"," +
            "\"y\": \"wnoj0YjheNP80XYh1SEvz1-wnKByEoHvb6KrDcjMuWc\"" +
            "\"crv\": \"P-256\"," +
            "}";

    private static final JsonWebKeySet ECDSA_SIGN_JWKS;

    private static final String ECDSA_VERIFY_JWK_JSON;

    private static final String KEY = "my-key";
    private static final String VALUE = "Message signed with JWK";

    // Copied from JwsSignatureBytebufValidatorTest
    static {
        try {
            // ECDSA
            PublicJsonWebKey ecdsaSignJwk = PublicJsonWebKey.Factory.newPublicJwk(ECDSA_SIGN_JWK_JSON);
            ECDSA_SIGN_JWKS = new JsonWebKeySet(ecdsaSignJwk);

            PublicJsonWebKey ecdsaVerifyJwk = PublicJsonWebKey.Factory.newPublicJwk(ECDSA_SIGN_JWK_JSON);
            ecdsaVerifyJwk.setKeyOps(List.of("verify"));
            ecdsaVerifyJwk.setPrivateKey(null);
            ECDSA_VERIFY_JWK_JSON = new JsonWebKeySet(ecdsaVerifyJwk).toJson();
        } catch (JoseException e) {
            throw new RuntimeException(e);
        }
    }

    private static final byte[] VALID_JWS_USING_ECDSA_JWK = generateJws(ECDSA_SIGN_JWKS, VALUE, false, false).getBytes(StandardCharsets.UTF_8);
    private static final RecordHeader VALID_JWS_USING_ECDSA_JWK_HEADER = new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK);

    @Test
    void validJwsProduceAccepted(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null, KEY, VALUE, List.of(VALID_JWS_USING_ECDSA_JWK_HEADER)));
            assertThatFutureSucceeds(result);

            var records = consumeAll(tester, topic);
            assertThat(records)
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(VALUE);

            assertThat(records)
                    .singleElement()
                    .satisfies(record -> isJwsHeaderMatching(record, VALID_JWS_USING_ECDSA_JWK));
        }
    }

    private static void isJwsHeaderMatching(ConsumerRecord<String, String> record, byte[] value) {
        assertThat(new RecordHeaders(record.headers()).lastHeader(JWS_HEADER_NAME).value()).isEqualTo(value);
    }

    // Copied from JwsSignatureBytebufValidatorTest
    private static String generateJws(JsonWebKeySet jwks, String payload, boolean isContentDetached, boolean useUnencodedPayload) {
        try {
            PublicJsonWebKey jwk = (PublicJsonWebKey) jwks.getJsonWebKeys().getFirst();

            JsonWebSignature jws = new JsonWebSignature();
            jws.setKeyIdHeaderValue(jwk.getKeyId());
            jws.setAlgorithmHeaderValue(jwk.getAlgorithm());
            jws.setKey(jwk.getPrivateKey());
            jws.setPayload(payload);

            if (useUnencodedPayload) {
                jws.setHeader(HeaderParameterNames.BASE64URL_ENCODE_PAYLOAD, false);
                jws.setCriticalHeaderNames(HeaderParameterNames.BASE64URL_ENCODE_PAYLOAD);
            }

            return isContentDetached ? jws.getDetachedContentCompactSerialization() : jws.getCompactSerialization();
        }
        catch (JoseException e) {
            throw new RuntimeException(e);
        }
    }

    private static ConfigurationBuilder createFilterDef(KafkaCluster cluster, Topic... topics) {
        String className = RecordValidation.class.getName();
        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className).withConfig("rules",
                        List.of(Map.of("topicNames", Arrays.stream(topics).map(Topic::name).toList(), "valueRule",
                                Map.of("jwsSignatureValidationConfig", Map.of("jsonWebKeySet", ECDSA_VERIFY_JWK_JSON, "algorithmConstraintType", "PERMIT", "algorithms", List.of("ES256"), "jwsHeaderName", JWS_HEADER_NAME)))))
                .build();

        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }
}
