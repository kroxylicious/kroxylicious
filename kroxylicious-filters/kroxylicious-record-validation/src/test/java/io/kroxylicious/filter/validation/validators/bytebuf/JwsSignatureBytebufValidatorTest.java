/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.validation.validators.Result;

import static io.kroxylicious.test.jws.JwsTestUtils.ECDSA_VERIFY_JWKS;
import static io.kroxylicious.test.jws.JwsTestUtils.INVALID_JWS;
import static io.kroxylicious.test.jws.JwsTestUtils.JWS_HEADER_NAME;
import static io.kroxylicious.test.jws.JwsTestUtils.RSA_AND_ECDSA_VERIFY_JWKS;
import static io.kroxylicious.test.jws.JwsTestUtils.RSA_VERIFY_JWKS;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_AND_CONTENT_DETACHED;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_AND_CONTENT_DETACHED_PAYLOAD;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_MISSING_ECDSA_JWK;
import static io.kroxylicious.test.jws.JwsTestUtils.VALID_JWS_USING_RSA_JWK;
import static io.kroxylicious.test.record.RecordTestUtils.record;
import static org.assertj.core.api.Assertions.assertThat;

public class JwsSignatureBytebufValidatorTest {
    // Recommended algorithms from https://datatracker.ietf.org/doc/html/rfc7518#section-3.1
    private static final AlgorithmConstraints PERMIT_ES256 = new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT,
            AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256);
    private static final AlgorithmConstraints PERMIT_RS256 = new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT, AlgorithmIdentifiers.RSA_USING_SHA256);
    private static final AlgorithmConstraints PERMIT_ES256_AND_RS256 = new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT,
            AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256, AlgorithmIdentifiers.RSA_USING_SHA256);

    private static final byte[] EMPTY = new byte[0];

    @Test
    void jwsSignedWithEcdsaJwkPassesValidation() {
        Record record = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK));
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_VERIFY_JWKS, PERMIT_ES256, JWS_HEADER_NAME, false);
        CompletionStage<Result> future = validator.validate(record.key(), record, true);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void jwsSignedWithRsaJwkPassesValidation() {
        Record record = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_RSA_JWK));
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(RSA_VERIFY_JWKS, PERMIT_RS256, JWS_HEADER_NAME, false);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void jwsSignedWithEcdsaJwkAndJwsSignedWithRsaJwkBothPassValidationWithNoConstraints() {
        Record ecdsaRecord = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK));

        BytebufValidator ecdsaValidator = BytebufValidators.jwsSignatureValidator(ECDSA_VERIFY_JWKS, AlgorithmConstraints.NO_CONSTRAINTS, JWS_HEADER_NAME, false);
        CompletionStage<Result> ecdsaFuture = ecdsaValidator.validate(ecdsaRecord.value(), ecdsaRecord, false);

        assertThat(ecdsaFuture)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);

        Record rsaRecord = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_RSA_JWK));

        BytebufValidator rsaValidator = BytebufValidators.jwsSignatureValidator(RSA_VERIFY_JWKS, AlgorithmConstraints.NO_CONSTRAINTS, JWS_HEADER_NAME, false);
        CompletionStage<Result> rsaFuture = rsaValidator.validate(rsaRecord.value(), rsaRecord, false);

        assertThat(rsaFuture)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void jwsSignedWithEcdsaJwkPassesValidationWithConstraintsAndMultiKeyJwks() {
        Record ecdsaRecord = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK));

        BytebufValidator ecdsaAndRsaValidator = BytebufValidators.jwsSignatureValidator(RSA_AND_ECDSA_VERIFY_JWKS, PERMIT_ES256, JWS_HEADER_NAME, false);
        CompletionStage<Result> ecdsaFuture = ecdsaAndRsaValidator.validate(ecdsaRecord.value(), ecdsaRecord, false);

        assertThat(ecdsaFuture)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void jwsSignedWithEcdsaJwkAndJwsSignedWithRsaJwkBothPassValidationWithConstraintsAndMultiKeyJwks() {
        Record ecdsaRecord = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK));
        Record rsaRecord = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_RSA_JWK));

        BytebufValidator ecdsaAndRsaValidator = BytebufValidators.jwsSignatureValidator(RSA_AND_ECDSA_VERIFY_JWKS, PERMIT_ES256_AND_RS256, JWS_HEADER_NAME, false);
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
    void jwsSignedWithEcdsaJwkAndUsingContentDetachedPassesValidation() {
        Record record = record(VALID_JWS_USING_ECDSA_JWK_AND_CONTENT_DETACHED_PAYLOAD, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK_AND_CONTENT_DETACHED));
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_VERIFY_JWKS, PERMIT_ES256, JWS_HEADER_NAME, true);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void jwsSignedWithEcdsaJwkAndUsingUnencodedContentDetachedPassesValidation() {
        Record record = record(VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD,
                new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED));
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_VERIFY_JWKS, PERMIT_ES256, JWS_HEADER_NAME, true);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(true, Result::valid);
    }

    @Test
    void invalidJwsFailsValidation() {
        Record record = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, INVALID_JWS));
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_VERIFY_JWKS, PERMIT_ES256, JWS_HEADER_NAME, false);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void missingJwsHeaderFailsValidation() {
        Record record = record(EMPTY);
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_VERIFY_JWKS, PERMIT_ES256, JWS_HEADER_NAME, false);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void jwsSignedWithMissingEcdsaJwkFailsValidation() {
        Record record = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_MISSING_ECDSA_JWK));
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_VERIFY_JWKS, PERMIT_ES256, JWS_HEADER_NAME, false);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void jwsSignedWithRsaJwkFailsValidationDueToEcdsaConstraints() {
        Record record = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_RSA_JWK));
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(RSA_VERIFY_JWKS, PERMIT_ES256, JWS_HEADER_NAME, false);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void jwsSignedWithEcdsaJwkAndUsingContentDetachedFailsValidationWithEmptyBuffer() {
        Record record = record(EMPTY, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK_AND_CONTENT_DETACHED));
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_VERIFY_JWKS, PERMIT_ES256, JWS_HEADER_NAME, true);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }

    @Test
    void jwsSignedWithEcdsaJwkAndUsingUnencodedContentDetachedFailsValidationWhenPayloadIsEncoded() {
        String base64EncodedPayload = Base64.getEncoder()
                .encodeToString(VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED_PAYLOAD.getBytes(StandardCharsets.UTF_8));
        Record record = record(base64EncodedPayload, new RecordHeader(JWS_HEADER_NAME, VALID_JWS_USING_ECDSA_JWK_AND_UNENCODED_CONTENT_DETACHED));
        BytebufValidator validator = BytebufValidators.jwsSignatureValidator(ECDSA_VERIFY_JWKS, PERMIT_ES256, JWS_HEADER_NAME, true);
        CompletionStage<Result> future = validator.validate(record.value(), record, false);

        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(1))
                .returns(false, Result::valid);
    }
}
