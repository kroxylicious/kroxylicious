/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import org.assertj.core.api.ThrowableAssert;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class Assertions {
    public static void throwsAssertionErrorContaining(ThrowableAssert.ThrowingCallable shouldRaiseThrowable, String description) {
        assertThatThrownBy(shouldRaiseThrowable).isInstanceOf(AssertionError.class).hasMessageContaining(description);
    }
}
