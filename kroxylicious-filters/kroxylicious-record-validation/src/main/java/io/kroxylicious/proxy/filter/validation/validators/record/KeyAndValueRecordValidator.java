/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.record;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.validation.validators.Result;
import io.kroxylicious.proxy.filter.validation.validators.bytebuf.BytebufValidator;

/**
 * Returns an invalid {@link Result} if either the key or value returns
 * an invalid result from a delegate validator
 */
public class KeyAndValueRecordValidator implements RecordValidator {

    private final BytebufValidator keyValidator;
    private final BytebufValidator valueValidator;

    private KeyAndValueRecordValidator(BytebufValidator keyValidator,
                                       BytebufValidator valueValidator) {
        if (keyValidator == null) {
            throw new IllegalArgumentException("keyValidator was null");
        }
        if (valueValidator == null) {
            throw new IllegalArgumentException("valueValidator was null");
        }
        this.keyValidator = keyValidator;
        this.valueValidator = valueValidator;
    }

    @Override
    public CompletionStage<Result> validate(Record record) {
        CompletionStage<Result> keyValid = keyValidator.validate(record.key(), record, true);
        return keyValid.thenCompose(result -> {
            if (!result.valid()) {
                return CompletableFuture.completedFuture(new Result(false, "Key was invalid: " + result.errorMessage()));
            }
            else {
                return validateValue(record);
            }
        });
    }

    private CompletionStage<Result> validateValue(Record record) {
        CompletionStage<Result> valueValid = valueValidator.validate(record.value(), record, false);
        return valueValid.thenCompose(result1 -> {
            if (!result1.valid()) {
                return CompletableFuture.completedFuture(new Result(false, "Value was invalid: " + result1.errorMessage()));
            }
            else {
                return Result.VALID_RESULT_STAGE;
            }
        });
    }

    /**
     * Get a RecordValidator that invalidates a record if either it's key or value fails validation with their respective validator
     * @param keyValidator validator for the record's key
     * @param valueValidator validator for the record's value
     * @return validator
     */
    public static RecordValidator keyAndValueValidator(BytebufValidator keyValidator, BytebufValidator valueValidator) {
        return new KeyAndValueRecordValidator(keyValidator, valueValidator);
    }
}
