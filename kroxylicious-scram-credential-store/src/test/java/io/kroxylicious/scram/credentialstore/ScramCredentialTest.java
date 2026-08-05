/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScramCredentialTest {

    @Test
    void shouldCreateValidCredential() {
        // Given
        byte[] salt = { 1, 2, 3, 4, 5 };
        byte[] serverKey = { 10, 20, 30, 40, 50 };
        byte[] storedKey = { 11, 21, 31, 41, 51 };

        // When
        ScramCredential credential = new ScramCredential(
                "alice",
                salt,
                4096,
                serverKey,
                storedKey,
                "SHA-256");

        // Then
        assertThat(credential.username()).isEqualTo("alice");
        assertThat(credential.salt()).isEqualTo(salt);
        assertThat(credential.iterations()).isEqualTo(4096);
        assertThat(credential.serverKey()).isEqualTo(serverKey);
        assertThat(credential.storedKey()).isEqualTo(storedKey);
        assertThat(credential.hashAlgorithm()).isEqualTo("SHA-256");
    }

    @Test
    void shouldAcceptSha512Algorithm() {
        assertThatCode(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-512"))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldAcceptHighIterationCount() {
        assertThatCode(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                10000,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256"))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldDefensivelyCopyArraysOnConstruction() {
        // Given
        byte[] salt = { 1, 2, 3 };
        byte[] serverKey = { 4, 5, 6 };
        byte[] storedKey = { 7, 8, 9 };
        ScramCredential credential = new ScramCredential("alice", salt, 4096, serverKey, storedKey, "SHA-256");

        // When
        salt[0] = 99;
        serverKey[0] = 99;
        storedKey[0] = 99;

        // Then
        assertThat(credential.salt()[0]).isEqualTo((byte) 1);
        assertThat(credential.serverKey()[0]).isEqualTo((byte) 4);
        assertThat(credential.storedKey()[0]).isEqualTo((byte) 7);
    }

    @Test
    void shouldDefensivelyCopyArraysOnAccess() {
        // Given
        ScramCredential credential = new ScramCredential(
                "alice", new byte[]{ 1, 2, 3 }, 4096, new byte[]{ 4, 5, 6 }, new byte[]{ 7, 8, 9 }, "SHA-256");

        // When
        credential.salt()[0] = 99;
        credential.serverKey()[0] = 99;
        credential.storedKey()[0] = 99;

        // Then
        assertThat(credential.salt()[0]).isEqualTo((byte) 1);
        assertThat(credential.serverKey()[0]).isEqualTo((byte) 4);
        assertThat(credential.storedKey()[0]).isEqualTo((byte) 7);
    }

    @SuppressWarnings("DataFlowIssue") // we're testing that the null argument is rejected
    @Test
    void shouldRejectNullUsername() {
        assertThatThrownBy(() -> new ScramCredential(
                null,
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("username");
    }

    @Test
    void shouldRejectEmptyUsername() {
        assertThatThrownBy(() -> new ScramCredential(
                "",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("username must not be empty");
    }

    @SuppressWarnings("DataFlowIssue") // we're testing that the null argument is rejected
    @Test
    void shouldRejectNullSalt() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                null,
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("salt");
    }

    @Test
    void shouldRejectEmptySalt() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                new byte[0],
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("salt must not be empty");
    }

    @SuppressWarnings("DataFlowIssue") // we're testing that the null argument is rejected
    @Test
    void shouldRejectNullServerKey() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                null,
                new byte[]{ 7, 8, 9 },
                "SHA-256"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("serverKey");
    }

    @Test
    void shouldRejectEmptyServerKey() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[0],
                new byte[]{ 7, 8, 9 },
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("serverKey must not be empty");
    }

    @SuppressWarnings("DataFlowIssue") // we're testing that the null argument is rejected
    @Test
    void shouldRejectNullStoredKey() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                null,
                "SHA-256"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("storedKey");
    }

    @Test
    void shouldRejectEmptyStoredKey() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[0],
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("storedKey must not be empty");
    }

    @SuppressWarnings("DataFlowIssue") // we're testing that the null argument is rejected
    @Test
    void shouldRejectNullHashAlgorithm() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("hashAlgorithm");
    }

    @Test
    void shouldRejectUnsupportedHashAlgorithm() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "MD5"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hashAlgorithm must be one of")
                .hasMessageContaining("SHA-256")
                .hasMessageContaining("SHA-512");
    }

    @Test
    void shouldRejectIterationsBelowMinimum() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4095,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("iterations must be at least 4096")
                .hasMessageContaining("4095");
    }

    @Test
    void shouldRejectNegativeIterations() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                -1,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("iterations must be at least 4096");
    }

    @Test
    void shouldRejectZeroIterations() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                0,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("iterations must be at least 4096");
    }

    @Test
    void shouldSupportRecordEquality() {
        ScramCredential credential1 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        assertThat(credential1).isEqualTo(credential2)
                .hasSameHashCodeAs(credential2);
    }

    @Test
    void shouldDetectInequality() {
        ScramCredential credential1 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "bob",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        assertThat(credential1).isNotEqualTo(credential2);
    }

    @Test
    void shouldDetectInequalityInSalt() {
        ScramCredential credential1 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "alice",
                new byte[]{ 9, 8, 7 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        assertThat(credential1).isNotEqualTo(credential2);
    }

    @Test
    void shouldDetectInequalityInIterations() {
        ScramCredential credential1 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                8192,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        assertThat(credential1).isNotEqualTo(credential2);
    }

    @Test
    void shouldDetectInequalityInServerKey() {
        ScramCredential credential1 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 9, 8, 7 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        assertThat(credential1).isNotEqualTo(credential2);
    }

    @Test
    void shouldDetectInequalityInStoredKey() {
        ScramCredential credential1 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 9, 8, 7 },
                "SHA-256");

        assertThat(credential1).isNotEqualTo(credential2);
    }

    @Test
    void shouldDetectInequalityInHashAlgorithm() {
        ScramCredential credential1 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-512");

        assertThat(credential1).isNotEqualTo(credential2);
    }

    @Test
    void shouldProduceConsistentHashCode() {
        ScramCredential credential = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        int hashCode1 = credential.hashCode();
        int hashCode2 = credential.hashCode();

        assertThat(hashCode1).isEqualTo(hashCode2);
    }

    @Test
    void shouldProvideMeaningfulToString() {
        ScramCredential credential = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        String toStringResult = credential.toString();

        assertThat(toStringResult)
                .contains("ScramCredential")
                .contains("alice")
                .contains("4096")
                .contains("SHA-256")
                .contains("salt=***")
                .contains("serverKey=***")
                .contains("storedKey=***");
    }
}
