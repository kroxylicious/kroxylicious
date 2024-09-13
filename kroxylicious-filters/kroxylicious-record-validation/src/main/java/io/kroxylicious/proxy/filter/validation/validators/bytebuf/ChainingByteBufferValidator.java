/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.validation.validators.Result;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A chain of {@link BytebufValidators}.  Validators are executed in the order
 * they are defined.  Validation stops after the first validation failure.
 */
class ChainingByteBufferValidator implements BytebufValidator {

    private final List<BytebufValidator> elements;

    ChainingByteBufferValidator(
            @NonNull
            List<BytebufValidator> elements
    ) {
        this.elements = List.copyOf(elements);
    }

    @Override
    public CompletionStage<Result> validate(ByteBuffer buffer, Record kafkaRecord, boolean isKey) {
        var future = Result.VALID_RESULT_STAGE;

        for (BytebufValidator bv : elements) {
            future = future.thenCompose(x -> {
                if (x.valid()) {
                    return bv.validate(buffer.asReadOnlyBuffer(), kafkaRecord, isKey);
                } else {
                    return CompletableFuture.completedStage(x);
                }
            });
        }
        return future;
    }
}
