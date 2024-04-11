/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.schema.validation.Result;
import io.kroxylicious.proxy.filter.schema.validation.TestRecords;

import static org.assertj.core.api.Assertions.assertThat;

class AllValidBytebufValidatorTest {

    @Test
    void allValid() {
        AllValidBytebufValidator allValidBytebufValidator = new AllValidBytebufValidator();
        CompletionStage<Result> valid = allValidBytebufValidator.validate(null, 0, TestRecords.createRecord("a", "b"), false);
        assertThat(valid).succeedsWithin(Duration.ZERO).isEqualTo(Result.VALID);
    }

}