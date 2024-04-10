/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf.apicurio;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.schema.validation.Result;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidator;

public class ApiCurioSchemaBytebufValidator implements BytebufValidator {

    private static final String valueSchemaHeader = "apicurio.value.globalId";
    private static final String keySchemaHeader = "apicurio.key.globalId";

    @Override
    public CompletionStage<Result> validate(ByteBuffer buffer, int length, Record record, boolean isKey) {
        // leave null validation to the NullEmptyBytebufValidator
        if (buffer == null) {
            return CompletableFuture.completedFuture(Result.VALID);
        }
        String header = isKey ? keySchemaHeader : valueSchemaHeader;
        Optional<Header> first = Arrays.stream(record.headers()).filter(h -> h.key().equals(header)).findFirst();
        if (first.isEmpty()) {
            return CompletableFuture.completedFuture(new Result(false, "record headers did not contain: " + header));
        }
        Header header1 = first.get();
        if (header1.value().length != 8) {
            return CompletableFuture.completedFuture(new Result(false, "header " + header + " value is not 8 bytes"));
        }
        return CompletableFuture.completedFuture(Result.VALID);
    }

}
