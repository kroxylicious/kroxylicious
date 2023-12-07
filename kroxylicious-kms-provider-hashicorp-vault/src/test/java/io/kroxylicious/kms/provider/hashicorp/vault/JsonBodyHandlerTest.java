/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.DataKeyData;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.DecryptData;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.ReadKeyData;

import static org.assertj.core.api.Assertions.assertThat;

class JsonBodyHandlerTest {

    static Stream<Arguments> responses() {
        return Stream.of(
                Arguments.of(
                        "readkey",
                        new TypeReference<VaultResponse<ReadKeyData>>() {
                        },
                        """
                                {
                                  "data": {
                                    "name": "foo",
                                    "latest_version": 1
                                  }
                                }""",
                        new VaultResponse<>(new ReadKeyData("foo", 1))),
                Arguments.of(
                        "readkey ignores unknown properties",
                        new TypeReference<VaultResponse<ReadKeyData>>() {
                        },
                        """
                                {
                                  "unknown1" : 123,
                                  "data": {
                                    "name": "foo",
                                    "latest_version": 1,
                                    "unknown2" : 456
                                  }
                                }""",
                        new VaultResponse<>(new ReadKeyData("foo", 1))),
                Arguments.of(
                        "decrypt",
                        new TypeReference<VaultResponse<DecryptData>>() {
                        },
                        """
                                {
                                  "data": {
                                    "plaintext": "foo"
                                  }
                                }""",
                        new VaultResponse<>(new DecryptData("foo"))),
                Arguments.of(
                        "decrypt ignores unknown properties",
                        new TypeReference<VaultResponse<DecryptData>>() {
                        },
                        """
                                {
                                  "data": {
                                    "plaintext": "foo",
                                    "unknown1": 123
                                  }
                                }""",
                        new VaultResponse<>(new DecryptData("foo"))),
                Arguments.of(
                        "datakey",
                        new TypeReference<VaultResponse<DataKeyData>>() {
                        },
                        """
                                {
                                  "data": {
                                    "plaintext": "plain",
                                    "ciphertext": "cipher"
                                  }
                                }""",
                        new VaultResponse<>(new DataKeyData("plain", "cipher"))),
                Arguments.of(
                        "datakey ignores unknown properties",
                        new TypeReference<VaultResponse<DataKeyData>>() {
                        },
                        """
                                {
                                  "data": {
                                    "plaintext": "plain",
                                    "ciphertext": "cipher",
                                    "unknown1": 123
                                  }
                                }""",
                        new VaultResponse<>(new DataKeyData("plain", "cipher")))

        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(value = "responses")
    <T> void handleResponse(String name, TypeReference<T> typeRef, String input, VaultResponse<T> expected) {
        var bodySubscriber = new JsonBodyHandler<>(typeRef).apply(null);

        bodySubscriber.onNext(List.of(ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8))));
        bodySubscriber.onComplete();

        var stage = bodySubscriber.getBody();
        assertThat(stage).isCompleted();

        var supplier = stage.toCompletableFuture().join();
        assertThat(supplier).extracting(Supplier::get).isEqualTo(expected);
    }

}
