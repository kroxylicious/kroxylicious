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
        ScramCredential credential = new ScramCredential(
                "alice",
                "FJz8jKVn7gKxR1wKGHXRXw==",
                4096,
                "yP4LXM+6b8SvL0N9i8fJQj5kJ5w=",
                "I8k2r9SvL0N9i8fJQj5kJ5wyP4L=",
                "SHA-256");

        assertThat(credential.username()).isEqualTo("alice");
        assertThat(credential.salt()).isEqualTo("FJz8jKVn7gKxR1wKGHXRXw==");
        assertThat(credential.iterations()).isEqualTo(4096);
        assertThat(credential.serverKey()).isEqualTo("yP4LXM+6b8SvL0N9i8fJQj5kJ5w=");
        assertThat(credential.storedKey()).isEqualTo("I8k2r9SvL0N9i8fJQj5kJ5wyP4L=");
        assertThat(credential.hashAlgorithm()).isEqualTo("SHA-256");
    }

    @Test
    void shouldAcceptSha512Algorithm() {
        assertThatCode(() -> new ScramCredential(
                "alice",
                "salt",
                4096,
                "serverKey",
                "storedKey",
                "SHA-512"))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldAcceptHighIterationCount() {
        assertThatCode(() -> new ScramCredential(
                "alice",
                "salt",
                10000,
                "serverKey",
                "storedKey",
                "SHA-256"))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectNullUsername() {
        assertThatThrownBy(() -> new ScramCredential(
                null,
                "salt",
                4096,
                "serverKey",
                "storedKey",
                "SHA-256"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("username");
    }

    @Test
    void shouldRejectEmptyUsername() {
        assertThatThrownBy(() -> new ScramCredential(
                "",
                "salt",
                4096,
                "serverKey",
                "storedKey",
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
                "serverKey",
                "storedKey",
                "SHA-256"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("salt");
    }

    @Test
    void shouldRejectEmptySalt() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                "",
                4096,
                "serverKey",
                "storedKey",
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("salt must not be empty");
    }

    @Test
    void shouldRejectNullServerKey() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                "salt",
                4096,
                null,
                "storedKey",
                "SHA-256"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("serverKey");
    }

    @Test
    void shouldRejectEmptyServerKey() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                "salt",
                4096,
                "",
                "storedKey",
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("serverKey must not be empty");
    }

    @Test
    void shouldRejectNullStoredKey() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                "salt",
                4096,
                "serverKey",
                null,
                "SHA-256"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("storedKey");
    }

    @Test
    void shouldRejectEmptyStoredKey() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                "salt",
                4096,
                "serverKey",
                "",
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("storedKey must not be empty");
    }

    @Test
    void shouldRejectNullHashAlgorithm() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                "salt",
                4096,
                "serverKey",
                "storedKey",
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("hashAlgorithm");
    }

    @Test
    void shouldRejectUnsupportedHashAlgorithm() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                "salt",
                4096,
                "serverKey",
                "storedKey",
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
                "salt",
                4095,
                "serverKey",
                "storedKey",
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("iterations must be at least 4096")
                .hasMessageContaining("4095");
    }

    @Test
    void shouldRejectNegativeIterations() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                "salt",
                -1,
                "serverKey",
                "storedKey",
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("iterations must be at least 4096");
    }

    @Test
    void shouldRejectZeroIterations() {
        assertThatThrownBy(() -> new ScramCredential(
                "alice",
                "salt",
                0,
                "serverKey",
                "storedKey",
                "SHA-256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("iterations must be at least 4096");
    }

    @Test
    void shouldSupportRecordEquality() {
        ScramCredential credential1 = new ScramCredential(
                "alice",
                "salt",
                4096,
                "serverKey",
                "storedKey",
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "alice",
                "salt",
                4096,
                "serverKey",
                "storedKey",
                "SHA-256");

        assertThat(credential1).isEqualTo(credential2);
        assertThat(credential1.hashCode()).isEqualTo(credential2.hashCode());
    }

    @Test
    void shouldDetectInequality() {
        ScramCredential credential1 = new ScramCredential(
                "alice",
                "salt",
                4096,
                "serverKey",
                "storedKey",
                "SHA-256");

        ScramCredential credential2 = new ScramCredential(
                "bob",
                "salt",
                4096,
                "serverKey",
                "storedKey",
                "SHA-256");

        assertThat(credential1).isNotEqualTo(credential2);
    }
}
