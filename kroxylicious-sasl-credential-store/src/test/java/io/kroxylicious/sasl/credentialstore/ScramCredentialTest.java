/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScramCredentialTest {

    @Test
    void shouldCreateValidCredential() {
        byte[] salt = { 1, 2, 3, 4, 5 };
        byte[] serverKey = { 10, 20, 30, 40, 50 };
        byte[] storedKey = { 11, 21, 31, 41, 51 };

        ScramCredential credential = new ScramCredential(
                "alice",
                salt,
                4096,
                serverKey,
                storedKey,
                "SHA-256");

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

        assertThat(credential1).isEqualTo(credential2);
        assertThat(credential1.hashCode()).isEqualTo(credential2.hashCode());
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
}
