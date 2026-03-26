/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.Test;

import io.kroxylicious.sasl.credentialstore.ScramCredential;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that credentials work correctly with Kafka's SCRAM implementation after round-tripping through JSON.
 */
class ScramCredentialRoundTripTest {

    @Test
    void shouldCreateCredentialsThatMatchKafkaScramFormatter() throws Exception {
        // Given - a known password and salt
        String password = "test-password";
        byte[] salt = new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
        int iterations = 4096;
        ScramMechanism mechanism = ScramMechanism.SCRAM_SHA_256;

        // When - we generate using ScramFormatter directly
        ScramFormatter formatter = new ScramFormatter(mechanism);
        byte[] saltedPassword = formatter.saltedPassword(password, salt, iterations);
        byte[] expectedServerKey = formatter.serverKey(saltedPassword);
        byte[] expectedClientKey = formatter.clientKey(saltedPassword);
        byte[] expectedStoredKey = formatter.storedKey(expectedClientKey);

        // And - create our ScramCredential
        ScramCredential credential = new ScramCredential(
                "testuser",
                salt,
                iterations,
                expectedServerKey,
                expectedStoredKey,
                "SHA-256");

        // Then - the credential should contain the exact same bytes
        assertThat(credential.salt()).isEqualTo(salt);
        assertThat(credential.serverKey()).isEqualTo(expectedServerKey);
        assertThat(credential.storedKey()).isEqualTo(expectedStoredKey);

        // And - after round-trip through JSON
        ScramCredentialSerializer serializer = new ScramCredentialSerializer();
        byte[] json = serializer.serialize(credential);
        ScramCredential deserialized = serializer.deserialize(json, "testuser");

        // Then - all bytes should match exactly
        assertThat(deserialized.salt()).isEqualTo(salt);
        assertThat(deserialized.serverKey()).isEqualTo(expectedServerKey);
        assertThat(deserialized.storedKey()).isEqualTo(expectedStoredKey);

        // And - when converted to Kafka's format (note: parameter order is salt, storedKey, serverKey, iterations)
        org.apache.kafka.common.security.scram.ScramCredential kafkaCredential = new org.apache.kafka.common.security.scram.ScramCredential(
                deserialized.salt(),
                deserialized.storedKey(),
                deserialized.serverKey(),
                deserialized.iterations());

        // Then - the values should match what we expect
        assertThat(kafkaCredential.salt()).isEqualTo(salt);
        assertThat(kafkaCredential.serverKey()).isEqualTo(expectedServerKey);
        assertThat(kafkaCredential.storedKey()).isEqualTo(expectedStoredKey);
        assertThat(kafkaCredential.iterations()).isEqualTo(iterations);
    }

    @Test
    void shouldPrintJsonFormatForDebugging() throws Exception {
        // This test helps us see what the JSON looks like
        ScramCredential credential = new ScramCredential(
                "alice",
                new byte[]{ 1, 2, 3 },
                4096,
                new byte[]{ 4, 5, 6 },
                new byte[]{ 7, 8, 9 },
                "SHA-256");

        ScramCredentialSerializer serializer = new ScramCredentialSerializer();
        byte[] json = serializer.serialize(credential);

        // Print for inspection
        System.out.println("JSON format: " + new String(json, java.nio.charset.StandardCharsets.UTF_8));

        // Verify it round-trips
        ScramCredential deserialized = serializer.deserialize(json, "alice");
        assertThat(deserialized.username()).isEqualTo("alice");
        assertThat(deserialized.salt()).isEqualTo(new byte[]{ 1, 2, 3 });
        assertThat(deserialized.serverKey()).isEqualTo(new byte[]{ 4, 5, 6 });
        assertThat(deserialized.storedKey()).isEqualTo(new byte[]{ 7, 8, 9 });
    }
}
