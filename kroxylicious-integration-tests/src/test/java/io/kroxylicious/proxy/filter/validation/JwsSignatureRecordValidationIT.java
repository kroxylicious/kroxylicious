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
    // Copied from JwsSignatureBytebufValidator
    private static final String DEFAULT_ERROR_MESSAGE = "JWS Signature could not be successfully verified";

    // Copied from JwsSignatureBytebufValidatorTest
    private static final String JWS_HEADER_NAME = "jws";

    private static final String ECDSA_SIGN_JWK_JSON = "{" +
            "\"kty\": \"EC\"," +
            "\"d\": \"Tk7qzHNnSBMioAU7NwZ9JugFWmWbUCyzeBRjVcTp_so\"," +
            "\"key_ops\": [ \"sign\" ]," +
            "\"alg\": \"ES256\"," +
            "\"crv\": \"P-256\"," +
            "\"kid\": \"ECDSA JWK\"," +
            "\"x\": \"qqeGjWmYZU5M5bBrRw1zqZcbPunoFVxsfaa9JdA0R5I\"," +
            "\"y\": \"wnoj0YjheNP80XYh1SEvz1-wnKByEoHvb6KrDcjMuWc\"" +
            "}";

    private static final String MISSING_ECDSA_SIGN_JWK_JSON = "{" +
            "\"kty\": \"EC\"," +
            "\"d\": \"-vdgJSAZ-Uorygv-ertyEcVZNacnWLG5DMY6xIYdYIA\"," +
            "\"key_ops\": [ \"sign\" ]," +
            "\"alg\": \"ES256\"," +
            "\"crv\": \"P-256\"," +
            "\"kid\": \"ECDSA MISSING JWK\"," +
            "\"x\": \"Y5fPO5Xy0TOVVfWeK3bAKjqcXT-RIiLnox29ACDmHsQ\"," +
            "\"y\": \"Js0Bt1PlHUrp5wMZXxJc97f8VoCZnuC7Z5pDNO7wNuc\"" +
            "}";

    private static final String RSA_SIGN_JWK_JSON = "{" +
            "\"kty\": \"RSA\"," +
            "\"d\": \"jIHlmSPqAasFF8kfLa6cZ8RY3g-iddn_P2vHhCGR9sXC-zP69TaRoq3ISnrosunAGHe9051VsAx2nFQFQ8Xqpl8CGr2lVov1vLt-43foHlc3XZc4lZ6MsmdPhx_7cq4xMygju8xGZ6bSp6BGDXCQsRNjqZL73SGEAxBX7rZUSV0jdcKYPqy6oAbTjGmhgWEMK8BCd6EfpydRWmrVJmDrGcx5lF_p8DvKNjwv2wEy6Y68KrDlp_nLpWkLdoa9XYiZyYm6SVS2IdIVbAVLzHdOb6XvVbu7aYk_ZREy9gGRdnPqiVHLomVFVJdd6Nisbgb6uAs6-8zfdoX6mSJP2pvIxQ\","
            +
            "\"key_ops\": [ \"sign\" ]," +
            "\"alg\": \"RS256\"," +
            "\"p\": \"xyPNtl3_2Rx1K1qlXX85U9Q1k75Ml1KOz2VXaqKaazEQ7Rp9RMAwuu0JUyGitQXQ5YKQm30CNZGkT3dlsZLHJNSgaiYPoJv_lQysn_QpezAVB2irTKrRnJCWyhkG30P2_W1c3kjVXbYKIczvYA8WbcHRgFEFhbcsDPbxZ1nvn-c\","
            +
            "\"q\": \"wkYuFficFLvp6wsUuF5_OzGnCHwJ6WyrHSHYTIF3iK_dSm4Y99k_lZY9WrICcx_3JVudiDtks5NzoaoQJLhfNgHAK5fcEZMhqdZ_1FebT7a1tJsXdmAQxQTmIhzRIAqe3muiugEuUvwRtX_BpLG7JVRjYlOZyDEMIZWi8A3g4B8\","
            +
            "\"e\":\"AQAB\"," +
            "\"kid\":\"RSA JWK\"," +
            "\"qi\":\"Xd7v96nYNfZKR7wmMjMmc3QLCMj609NPsmYbeYJSwpUKux37G9KkMzW_K9VJ7XRPPJp00CbtTnkyoxzIgZbFh-U9LZhccaCEuVc_Kei-1oGjyCbTJKvveTzzEGC7UBSKld8SnUtuO8JsDimsZ7-DHJHeZi-tXfNTuwdIr5cbJZA\","
            +
            "\"dp\":\"IIcvpfdKwFsOpItE8bXDVncWXVC7UAhzPVtPYSK4WIQGQMSP67f8_buUR1j6K9mMWsDuAAf2YWutzDEzkkLodpKotU4MRW7V27HbTLFkSTP8a15khLxuSsWva8mUvslqQdEoV0LMX2dJ1mWUQDuWrUz4fJ4_aa0W6_M2UWx2YMc\","
            +
            "\"dq\":\"vSheCRCG0H1jNnMUmquP0E_5Jf64G-qt9XCVzXAlthYeLjFi6DhEe97MIHnAft2540r_6LyDwYGpjdgrXcWTFt-_f_Kd2RLcLSToVBV06Lmq1I5J2v2QdnTdqotKZ5tPsps010z9ENnUWFdrcXOIF8HB_uQNkOmIuU6cVoX81ds\","
            +
            "\"n\":\"lx-5h_lkVJGc8-NxgKGkfxobBlM7DDCjqAloEieCf8c9KbPJ12V0Bm8arOX-lm0wRSLxmWzQ3NTM6S9pDbSc7fQt77THlMD1zEDMwIAJxfoMt3U_VrMHVdiOvO6YpU3jWBp7BfQNYHJjCSiaxIHqDPuDCh2GTflkU32Nfim7W3iLBuH4_K8ADYHz3QdeX29vzl49PDbUiiJEsjnEconsfuYjrFaN3uGn41mGzjedc7oxlcID8fDxq5bvZvOFBAIqdrxs5qPYw1QlVupB3LT0AMU1qKWOB_vkM1n54eDIxP9Xd__WnvX4wagYqA1OZ9LPBenhBX7_Rbnrap2QNQ58-Q\""
            +
            "}";

    private static final JsonWebKeySet ECDSA_SIGN_JWKS;
    public static final JsonWebKeySet ECDSA_VERIFY_JWKS;

    private static final JsonWebKeySet MISSING_ECDSA_SIGN_JWKS;

    private static final JsonWebKeySet RSA_SIGN_JWKS;

    public static final JsonWebKeySet RSA_AND_ECDSA_VERIFY_JWKS;

    static {
        try {
            // ECDSA
            PublicJsonWebKey ecdsaSignJwk = PublicJsonWebKey.Factory.newPublicJwk(ECDSA_SIGN_JWK_JSON);
            ECDSA_SIGN_JWKS = new JsonWebKeySet(ecdsaSignJwk);

            // Note: kid cannot change https://bitbucket.org/b_c/jose4j/src/jose4j-0.9.6/src/main/java/org/jose4j/jwk/SimpleJwkFilter.java#lines-103
            PublicJsonWebKey ecdsaVerifyJwk = PublicJsonWebKey.Factory.newPublicJwk(ECDSA_SIGN_JWK_JSON);
            ecdsaVerifyJwk.setKeyOps(List.of("verify"));
            ecdsaVerifyJwk.setPrivateKey(null);
            ECDSA_VERIFY_JWKS = new JsonWebKeySet(ecdsaVerifyJwk);

            // Missing ECDSA
            PublicJsonWebKey missingEcdsaJwk = PublicJsonWebKey.Factory.newPublicJwk(MISSING_ECDSA_SIGN_JWK_JSON);
            MISSING_ECDSA_SIGN_JWKS = new JsonWebKeySet(missingEcdsaJwk);

            // RSA
            PublicJsonWebKey rsaSignJwk = PublicJsonWebKey.Factory.newPublicJwk(RSA_SIGN_JWK_JSON);
            RSA_SIGN_JWKS = new JsonWebKeySet(rsaSignJwk);

            // Note: kid cannot change https://bitbucket.org/b_c/jose4j/src/jose4j-0.9.6/src/main/java/org/jose4j/jwk/SimpleJwkFilter.java#lines-103
            PublicJsonWebKey rsaVerifyJwk = PublicJsonWebKey.Factory.newPublicJwk(RSA_SIGN_JWK_JSON);
            rsaVerifyJwk.setKeyOps(List.of("verify"));
            rsaVerifyJwk.setPrivateKey(null);

            // RSA and ECDSA
            RSA_AND_ECDSA_VERIFY_JWKS = new JsonWebKeySet(rsaVerifyJwk, ecdsaVerifyJwk);
        }
        catch (JoseException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String VALID_JWS_USING_ECDSA_JWK_PAYLOAD = "Message signed with JWK";
    private static final byte[] VALID_JWS_USING_ECDSA_JWK = generateJws(ECDSA_SIGN_JWKS, VALID_JWS_USING_ECDSA_JWK_PAYLOAD, false, false)
            .getBytes(StandardCharsets.UTF_8);

    private static final String VALID_JWS_USING_MISSING_ECDSA_JWK_PAYLOAD = "Message signed with missing JWK";
    private static final byte[] VALID_JWS_USING_MISSING_ECDSA_JWK = generateJws(MISSING_ECDSA_SIGN_JWKS, VALID_JWS_USING_MISSING_ECDSA_JWK_PAYLOAD, false, false)
            .getBytes(StandardCharsets.UTF_8);

    private static final String VALID_JWS_USING_RSA_JWK_PAYLOAD = "Message signed with RSA JWK";
    private static final byte[] VALID_JWS_USING_RSA_JWK = generateJws(RSA_SIGN_JWKS, VALID_JWS_USING_RSA_JWK_PAYLOAD, false, false).getBytes(StandardCharsets.UTF_8);

    private static final String VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD = "Message signed with JWK that will be content detached and unencoded";
    private static final byte[] VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED = generateJws(ECDSA_SIGN_JWKS,
            VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD, true, true)
            .getBytes(StandardCharsets.UTF_8);

    private static final byte[] INVALID_JWS = "This is a non JWS value".getBytes(StandardCharsets.UTF_8);
    private static final String RANDOM_STRING = "random string";
    private static final String EMPTY_STRING = "";

    @Test
    void validJwsProduceAccepted(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, false);

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
                    .satisfies(record -> isJwsHeaderMatching(record, VALID_JWS_USING_ECDSA_JWK));
        }
    }

    @Test
    void validJwsSignedWithJwkFromMultiKeyJwksProduceAccepted(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, true, false);

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
                    .satisfies(record -> isJwsHeaderMatching(record, VALID_JWS_USING_RSA_JWK));
        }
    }

    @Test
    void validDetachedContentJwsProduceAccepted(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, true);

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
                    .satisfies(record -> isJwsHeaderMatching(record, VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED));
        }
    }

    @Test
    void invalidJwsProduceRejected(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, RANDOM_STRING, List.of(new RecordHeader(JWS_HEADER_NAME, INVALID_JWS))));
            assertThatFutureFails(result, InvalidRecordException.class, DEFAULT_ERROR_MESSAGE + ": A JWS Compact Serialization must have exactly 3 parts");
        }
    }

    @Test
    void jwsSignedWithMissingJwkProduceRejected(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, RANDOM_STRING, List.of(new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_MISSING_ECDSA_JWK))));
            assertThatFutureFails(result, InvalidRecordException.class, DEFAULT_ERROR_MESSAGE + ": Could not select valid JWK that matches the algorithm constraints");
        }
    }

    @Test
    void jwsSignedWithRsaJwkProduceRejectedDueToEcdsaConstraints(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, false);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, VALID_JWS_USING_RSA_JWK_PAYLOAD, List.of(new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_RSA_JWK))));
            assertThatFutureFails(result, InvalidRecordException.class, DEFAULT_ERROR_MESSAGE + ": Could not select valid JWK that matches the algorithm constraints");
        }
    }

    @Test
    void invalidDetachedContentJwsProduceRejected(KafkaCluster cluster, Topic topic) {
        ConfigurationBuilder config = createFilterDef(cluster, topic, false, true);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), null, null,
                    EMPTY_STRING, RANDOM_STRING, List.of(new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED))));
            assertThatFutureFails(result, InvalidRecordException.class, DEFAULT_ERROR_MESSAGE + ": JWS Signature was invalid");
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

    private static ConfigurationBuilder createFilterDef(KafkaCluster cluster, Topic topic, boolean multiKeyJwks, boolean contentDetached) {
        String className = RecordValidation.class.getName();

        Map<String, Object> jwsConfig;

        if (multiKeyJwks) {
            jwsConfig = Map.of("jsonWebKeySet", RSA_AND_ECDSA_VERIFY_JWKS.toJson(), "algorithmConstraintType", "PERMIT", "algorithms", List.of("ES256", "RS256"),
                    "jwsHeaderName",
                    JWS_HEADER_NAME, "isContentDetached", contentDetached);
        }
        else {
            jwsConfig = Map.of("jsonWebKeySet", ECDSA_VERIFY_JWKS.toJson(), "algorithmConstraintType", "PERMIT", "algorithms", List.of("ES256"), "jwsHeaderName",
                    JWS_HEADER_NAME, "isContentDetached", contentDetached);
        }

        NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(className, className).withConfig("rules",
                List.of(Map.of("topicNames", List.of(topic.name()), "valueRule",
                        Map.of("jwsSignatureValidationConfig", jwsConfig))))
                .build();

        return proxy(cluster)
                .addToFilterDefinitions(namedFilterDefinition)
                .addToDefaultFilters(namedFilterDefinition.name());
    }
}
