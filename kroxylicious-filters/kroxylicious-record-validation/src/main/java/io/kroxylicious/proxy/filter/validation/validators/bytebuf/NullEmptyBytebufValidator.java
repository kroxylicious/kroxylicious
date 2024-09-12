/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.validation.validators.Result;

class NullEmptyBytebufValidator implements BytebufValidator {

    private final boolean nullValid;
    private final boolean emptyValid;
    private final BytebufValidator delegate;

    NullEmptyBytebufValidator(boolean nullValid, boolean emptyValid, BytebufValidator delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate is null");
        }
        this.nullValid = nullValid;
        this.emptyValid = emptyValid;
        this.delegate = delegate;
    }

    @Override
    public CompletionStage<Result> validate(ByteBuffer buffer, Record record, boolean isKey) {
        var result = isKey ? validateField(record.hasKey(), record.keySize()) : validateField(record.hasValue(), record.valueSize());
        if (result != null) {
            return CompletableFuture.completedStage(result);
        }
        return delegate.validate(buffer, record, isKey);
    }

    private Result validateField(boolean hasField, int fieldLenth) {
        if (!hasField) {
            return nullValid ? Result.VALID_RESULT : new Result(false, "Null buffer invalid");
        } else if (fieldLenth <= 0) {
            return emptyValid ? Result.VALID_RESULT : new Result(false, "Empty buffer invalid");
        }
        return null;
    }

}
