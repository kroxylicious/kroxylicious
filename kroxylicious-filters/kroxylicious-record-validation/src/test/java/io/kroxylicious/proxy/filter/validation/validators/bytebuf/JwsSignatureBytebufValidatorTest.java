/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.record.Record;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.lang.JoseException;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.validation.validators.Result;

import static io.kroxylicious.test.record.RecordTestUtils.record;
import static org.assertj.core.api.Assertions.assertThat;

public class JwsSignatureBytebufValidatorTest {
    // Recommended algorithms from https://datatracker.ietf.org/doc/html/rfc7518#section-3.1
    private static final AlgorithmConstraints PERMIT_ES256 = new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT,
            AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256);
    private static final AlgorithmConstraints PERMIT_RS256 = new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT, AlgorithmIdentifiers.RSA_USING_SHA256);
    private static final AlgorithmConstraints PERMIT_ES256_AND_RS256 = new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT,
            AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256, AlgorithmIdentifiers.RSA_USING_SHA256);

    // Note: These include both public and private keys for simpler JWS generation and verification in the tests.
    // In production, only public keys should be passed to the validator.
    public static final JsonWebKeySet ECDSA_JWKS;
    private static final JsonWebKeySet MISSING_ECDSA_JWKS;
    private static final JsonWebKeySet RSA_JWKS;
    public static final JsonWebKeySet RSA_AND_ECDSA_JWKS;

    static {
        try {
            PublicJsonWebKey ecdsaJwk = PublicJsonWebKey.Factory.newPublicJwk("{" +
                    "\"kty\": \"EC\"," +
                    "\"d\": \"Tk7qzHNnSBMioAU7NwZ9JugFWmWbUCyzeBRjVcTp_so\"," +
                    "\"use\": \"sig\"," +
                    "\"alg\": \"ES256\"," +
                    "\"crv\": \"P-256\"," +
                    "\"kid\": \"Trusted ECDSA JWK\"," +
                    "\"x\": \"qqeGjWmYZU5M5bBrRw1zqZcbPunoFVxsfaa9JdA0R5I\"," +
                    "\"y\": \"wnoj0YjheNP80XYh1SEvz1-wnKByEoHvb6KrDcjMuWc\"" +
                    "}");

            ECDSA_JWKS = new JsonWebKeySet(ecdsaJwk);

            PublicJsonWebKey missingEcdsaJwk = PublicJsonWebKey.Factory.newPublicJwk("{" +
                    "\"kty\": \"EC\"," +
                    "\"d\": \"-vdgJSAZ-Uorygv-ertyEcVZNacnWLG5DMY6xIYdYIA\"," +
                    "\"use\": \"sig\"," +
                    "\"alg\": \"ES256\"," +
                    "\"crv\": \"P-256\"," +
                    "\"kid\": \"Untrusted ECDSA JWK\"," +
                    "\"x\": \"Y5fPO5Xy0TOVVfWeK3bAKjqcXT-RIiLnox29ACDmHsQ\"," +
                    "\"y\": \"Js0Bt1PlHUrp5wMZXxJc97f8VoCZnuC7Z5pDNO7wNuc\"" +
                    "}");

            MISSING_ECDSA_JWKS = new JsonWebKeySet(missingEcdsaJwk);

            PublicJsonWebKey rsaJwk = PublicJsonWebKey.Factory.newPublicJwk("{" +
                    "\"kty\": \"RSA\"," +
                    "\"d\": \"jIHlmSPqAasFF8kfLa6cZ8RY3g-iddn_P2vHhCGR9sXC-zP69TaRoq3ISnrosunAGHe9051VsAx2nFQFQ8Xqpl8CGr2lVov1vLt-43foHlc3XZc4lZ6MsmdPhx_7cq4xMygju8xGZ6bSp6BGDXCQsRNjqZL73SGEAxBX7rZUSV0jdcKYPqy6oAbTjGmhgWEMK8BCd6EfpydRWmrVJmDrGcx5lF_p8DvKNjwv2wEy6Y68KrDlp_nLpWkLdoa9XYiZyYm6SVS2IdIVbAVLzHdOb6XvVbu7aYk_ZREy9gGRdnPqiVHLomVFVJdd6Nisbgb6uAs6-8zfdoX6mSJP2pvIxQ\","
                    +
                    "\"use\":\"sig\"," +
                    "\"alg\": \"RS256\"," +
                    "\"p\": \"xyPNtl3_2Rx1K1qlXX85U9Q1k75Ml1KOz2VXaqKaazEQ7Rp9RMAwuu0JUyGitQXQ5YKQm30CNZGkT3dlsZLHJNSgaiYPoJv_lQysn_QpezAVB2irTKrRnJCWyhkG30P2_W1c3kjVXbYKIczvYA8WbcHRgFEFhbcsDPbxZ1nvn-c\","
                    +
                    "\"q\": \"wkYuFficFLvp6wsUuF5_OzGnCHwJ6WyrHSHYTIF3iK_dSm4Y99k_lZY9WrICcx_3JVudiDtks5NzoaoQJLhfNgHAK5fcEZMhqdZ_1FebT7a1tJsXdmAQxQTmIhzRIAqe3muiugEuUvwRtX_BpLG7JVRjYlOZyDEMIZWi8A3g4B8\","
                    +
                    "\"e\":\"AQAB\"," +
                    "\"kid\":\"Trusted RSA JWK\"," +
                    "\"qi\":\"Xd7v96nYNfZKR7wmMjMmc3QLCMj609NPsmYbeYJSwpUKux37G9KkMzW_K9VJ7XRPPJp00CbtTnkyoxzIgZbFh-U9LZhccaCEuVc_Kei-1oGjyCbTJKvveTzzEGC7UBSKld8SnUtuO8JsDimsZ7-DHJHeZi-tXfNTuwdIr5cbJZA\","
                    +
                    "\"dp\":\"IIcvpfdKwFsOpItE8bXDVncWXVC7UAhzPVtPYSK4WIQGQMSP67f8_buUR1j6K9mMWsDuAAf2YWutzDEzkkLodpKotU4MRW7V27HbTLFkSTP8a15khLxuSsWva8mUvslqQdEoV0LMX2dJ1mWUQDuWrUz4fJ4_aa0W6_M2UWx2YMc\","
                    +
                    "\"dq\":\"vSheCRCG0H1jNnMUmquP0E_5Jf64G-qt9XCVzXAlthYeLjFi6DhEe97MIHnAft2540r_6LyDwYGpjdgrXcWTFt-_f_Kd2RLcLSToVBV06Lmq1I5J2v2QdnTdqotKZ5tPsps010z9ENnUWFdrcXOIF8HB_uQNkOmIuU6cVoX81ds\","
                    +
                    "\"n\":\"lx-5h_lkVJGc8-NxgKGkfxobBlM7DDCjqAloEieCf8c9KbPJ12V0Bm8arOX-lm0wRSLxmWzQ3NTM6S9pDbSc7fQt77THlMD1zEDMwIAJxfoMt3U_VrMHVdiOvO6YpU3jWBp7BfQNYHJjCSiaxIHqDPuDCh2GTflkU32Nfim7W3iLBuH4_K8ADYHz3QdeX29vzl49PDbUiiJEsjnEconsfuYjrFaN3uGn41mGzjedc7oxlcID8fDxq5bvZvOFBAIqdrxs5qPYw1QlVupB3LT0AMU1qKWOB_vkM1n54eDIxP9Xd__WnvX4wagYqA1OZ9LPBenhBX7_Rbnrap2QNQ58-Q\""
                    +
                    "}");

            RSA_JWKS = new JsonWebKeySet(rsaJwk);

            RSA_AND_ECDSA_JWKS = new JsonWebKeySet(rsaJwk, ecdsaJwk);
        }
        catch (JoseException e) {
            throw new RuntimeException(e);
        }
    }

    private static final byte[] VALID_JWS_USING_ECDSA_JWK = generateJws(ECDSA_JWKS, "Message signed with JWK").getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALID_JWS_USING_MISSING_ECDSA_JWK = generateJws(MISSING_ECDSA_JWKS, "Message signed with missing JWK").getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALID_JWS_USING_RSA_JWK = generateJws(RSA_JWKS, "Message signed with RSA JWK").getBytes(StandardCharsets.UTF_8);

    private static final byte[] NON_JWS_VALUE = "This is a non JWS value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY = new byte[0];

    @Test
    void keySignedWithEcdsaJwkPassesJwsSignatureValidation() {
        Record record = record(VALID_JWS_USING_ECDSA_JWK, EMPTY);
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_JWKS, PERMIT_ES256);
        CompletionStage<Result> future = validator.validate(record.key(), record, true);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valueSignedWithEcdsaJwkPassesJwsSignatureValidation() {
        Record record = record(EMPTY, VALID_JWS_USING_ECDSA_JWK);
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_JWKS, PERMIT_ES256);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valueSignedWithRsaJwkPassesJwsSignatureValidation() {
        Record record = record(EMPTY, VALID_JWS_USING_RSA_JWK);
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(RSA_JWKS, PERMIT_RS256);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valuesSignedWithEcdsaJwkAndValueSignedWithRsaJwkBothPassJwsSignatureValidationWithNoConstraints() {
        Record ecdsaRecord = record(EMPTY, VALID_JWS_USING_ECDSA_JWK);

        BytebufValidator ecdsaValidator = BytebufValidators.jwsSignatureValidator(ECDSA_JWKS, AlgorithmConstraints.NO_CONSTRAINTS);
        CompletionStage<Result> ecdsaFuture = ecdsaValidator.validate(ecdsaRecord.value(), ecdsaRecord, false);

        assertThat(ecdsaFuture)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);

        Record rsaRecord = record(EMPTY, VALID_JWS_USING_RSA_JWK);

        BytebufValidator rsaValidator = BytebufValidators.jwsSignatureValidator(RSA_JWKS, AlgorithmConstraints.NO_CONSTRAINTS);
        CompletionStage<Result> rsaFuture = rsaValidator.validate(rsaRecord.value(), rsaRecord, false);

        assertThat(rsaFuture)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valueSignedWithEcdsaJwkPassesJwsSignatureValidationWithConstraintsAndMultiKeyJwks() {
        Record ecdsaRecord = record(EMPTY, VALID_JWS_USING_ECDSA_JWK);

        BytebufValidator ecdsaAndRsaValidator = BytebufValidators.jwsSignatureValidator(RSA_AND_ECDSA_JWKS, PERMIT_ES256);
        CompletionStage<Result> ecdsaFuture = ecdsaAndRsaValidator.validate(ecdsaRecord.value(), ecdsaRecord, false);

        assertThat(ecdsaFuture)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void valuesSignedWithEcdsaJwkAndValueSignedWithRsaJwkBothPassJwsSignatureValidationWithConstraintsAndMultiKeyJwks() {
        Record ecdsaRecord = record(EMPTY, VALID_JWS_USING_ECDSA_JWK);
        Record rsaRecord = record(EMPTY, VALID_JWS_USING_RSA_JWK);

        BytebufValidator ecdsaAndRsaValidator = BytebufValidators.jwsSignatureValidator(RSA_AND_ECDSA_JWKS, PERMIT_ES256_AND_RS256);
        CompletionStage<Result> ecdsaFuture = ecdsaAndRsaValidator.validate(ecdsaRecord.value(), ecdsaRecord, false);
        CompletionStage<Result> rsaFuture = ecdsaAndRsaValidator.validate(rsaRecord.value(), rsaRecord, false);

        assertThat(ecdsaFuture)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);

        assertThat(rsaFuture)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void nonJwsSignatureValueFailsJwsSignatureValidation() {
        Record record = record(EMPTY, NON_JWS_VALUE);
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_JWKS, PERMIT_ES256);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void valueSignedWithMissingEcdsaJwkFailsJwsSignatureValidation() {
        Record record = record(EMPTY, VALID_JWS_USING_MISSING_ECDSA_JWK);
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_JWKS, PERMIT_ES256);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void valueSignedWithRsaJwkFailsJwsSignatureValidationDueToEcdsaConstraints() {
        Record record = record(EMPTY, VALID_JWS_USING_RSA_JWK);
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(RSA_JWKS, PERMIT_ES256);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    private static String generateJws(JsonWebKeySet jwks, String payload) {
        try {
            PublicJsonWebKey jwk = (PublicJsonWebKey) jwks.getJsonWebKeys().get(0);

            JsonWebSignature jws = new JsonWebSignature();
            jws.setKeyIdHeaderValue(jwk.getKeyId());
            jws.setAlgorithmHeaderValue(jwk.getAlgorithm());
            jws.setKey(jwk.getPrivateKey());
            jws.setPayload(payload);

            return jws.getCompactSerialization();
        }
        catch (JoseException e) {
            throw new RuntimeException(e);
        }
    }
}