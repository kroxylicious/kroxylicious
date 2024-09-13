/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.topic;

import java.util.List;

/**
 * Result of the validation of a single partition from a ProduceRequestData
 * @param index index of the partition
 * @param recordValidationFailures details of any records that failed validation
 */
public record PartitionValidationResult(
        int index,
        List<RecordValidationFailure> recordValidationFailures
) {

    /**
     * Are all records in the partition valid?
     * @return true if all records in the partition are valid
     */
    public boolean allRecordsValid() {
        return recordValidationFailures.isEmpty();
    }
}
