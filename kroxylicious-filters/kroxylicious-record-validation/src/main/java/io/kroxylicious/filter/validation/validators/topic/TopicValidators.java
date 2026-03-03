/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.validators.topic;

import io.kroxylicious.filter.validation.validators.record.RecordValidator;

/**
 * Factory for different topic {@link TopicValidator} implementations
 */
public class TopicValidators {

    private TopicValidators() {
    }

    private static final AllValidTopicValidator ALL_VALID = new AllValidTopicValidator();

    /**
     * A validator that always validates any input topic data
     * @return validator
     */
    public static TopicValidator allValid() {
        return ALL_VALID;
    }

    /**
     * A validator that tests the records of a topic produce data and invalidates if any records are invalid
     * @param validator a validator
     * @return per-record topic validator
     */
    public static TopicValidator perRecordValidator(RecordValidator validator) {
        return new PerRecordTopicValidator(validator);
    }

}
