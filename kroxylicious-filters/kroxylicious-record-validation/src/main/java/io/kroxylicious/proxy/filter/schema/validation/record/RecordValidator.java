/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.record;

import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.schema.validation.Result;

/**
 * Validator for individual ${@link Record}s
 */
public interface RecordValidator {

    /**
     * Validate the record
     * @param record the record to be validated
     * @return a Result describing if the record is valid and any failure message/exception if it is not.
     */
    Result validate(Record record);
}
