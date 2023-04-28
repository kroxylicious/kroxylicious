/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.record;

import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.schema.validation.Result;
import io.kroxylicious.proxy.filter.schema.validation.bytebuf.BytebufValidator;

/**
 * Returns an invalid {@link Result} if either the key or value returns
 * an invalid result from a delegate validator
 */
public class KeyAndValueRecordValidator implements RecordValidator {

    private final BytebufValidator keyValidator;
    private final BytebufValidator valueValidator;

    private KeyAndValueRecordValidator(BytebufValidator keyValidator, BytebufValidator valueValidator) {
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
    public Result validate(Record record) {
        Result keyValid = keyValidator.validate(record.key(), record.keySize(), record, true);
        if (!keyValid.valid()) {
            return new Result(false, "Key was invalid: " + keyValid.errorMessage());
        }
        Result valueValid = valueValidator.validate(record.value(), record.valueSize(), record, false);
        if (!valueValid.valid()) {
            return new Result(false, "Value was invalid: " + valueValid.errorMessage());
        }
        return Result.VALID;
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
