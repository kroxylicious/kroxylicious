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

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.ReadKeyData;

import static org.assertj.core.api.Assertions.assertThat;

class JsonBodyHandlerTest {

    @Test
    void handleReadKeyResponse() {
        var bodySubscriber = new JsonBodyHandler<VaultResponse<ReadKeyData>>(new TypeReference<>() {
        }).apply(null);

        bodySubscriber.onNext(List.of(ByteBuffer.wrap(
                """
                        {
                          "data": {
                            "name": "foo",
                            "latest_version": 1
                          }
                        }""".getBytes(StandardCharsets.UTF_8))));
        bodySubscriber.onComplete();

        var stage = bodySubscriber.getBody();
        assertThat(stage).isCompleted();
        var supplier = stage.toCompletableFuture().join();

        var expected = new VaultResponse<>(new ReadKeyData("foo", 1));

        assertThat(supplier).extracting(Supplier::get).isEqualTo(expected);
    }

}
