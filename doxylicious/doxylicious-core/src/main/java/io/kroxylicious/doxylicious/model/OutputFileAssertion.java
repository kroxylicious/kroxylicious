/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import java.util.List;

import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import edu.umd.cs.findbugs.annotations.Nullable;

public record OutputFileAssertion(
                                  @Nullable String contains,
                                  @JsonSetter(nulls = Nulls.AS_EMPTY) List<String> containsAll,
                                  @JsonSetter(nulls = Nulls.AS_EMPTY) List<String> containsAny,
                                  @Nullable String doesNotContain) {
    @SuppressWarnings("java:S5960") // assertions production code in libraries used for testing are OK
    public void makeAssertion(String description, String content) {
        AbstractStringAssert<?> assert_ = Assertions.assertThat(content).describedAs(description);
        if (contains != null) {
            assert_.contains(contains);
        }
        if (containsAll != null && !containsAll.isEmpty()) {
            assert_.contains(containsAll);
        }
        if (containsAny != null && !containsAny.isEmpty()) {
            assert_.containsAnyOf(containsAny.toArray(new String[0]));
        }
        if (doesNotContain != null) {
            assert_.doesNotContain(doesNotContain);
        }
    }
}
