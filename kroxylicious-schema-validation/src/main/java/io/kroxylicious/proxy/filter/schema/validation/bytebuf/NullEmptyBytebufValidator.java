/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

import java.nio.ByteBuffer;

import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.schema.validation.Result;

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
    public Result validate(ByteBuffer buffer, int length, Record record, boolean isKey) {
        if (buffer == null) {
            return result(nullValid, "Null buffer invalid");
        }
        else if (length == 0) {
            return result(emptyValid, "Empty buffer invalid");
        }
        return delegate.validate(buffer, length, record, isKey);
    }

    private Result result(boolean allowed, String message) {
        return allowed ? Result.VALID : new Result(false, message);
    }
}
