/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutputFileAssertionTest {

    @Test
    void testContains() {
        var a = new OutputFileAssertion("foo", List.of(), List.of(), null);
        assertThatThrownBy(() -> a.makeAssertion("desc", "fo fo")).isExactlyInstanceOf(AssertionError.class);
        Assertions.assertThatCode(() -> a.makeAssertion("desc", "fo foo")).doesNotThrowAnyException();
    }

    @Test
    void testContainsAll() {
        var a = new OutputFileAssertion("foo", List.of("foo", "bar"), List.of(), null);
        assertThatThrownBy(() -> a.makeAssertion("desc", "ba foo")).isExactlyInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> a.makeAssertion("desc", "bar fo")).isExactlyInstanceOf(AssertionError.class);
        Assertions.assertThatCode(() -> a.makeAssertion("desc", "bar foo")).doesNotThrowAnyException();
    }

    @Test
    void testContainsAny() {
        var a = new OutputFileAssertion(null, List.of(), List.of("foo", "bar"), null);
        assertThatThrownBy(() -> a.makeAssertion("desc", "fo fo")).isExactlyInstanceOf(AssertionError.class);
        Assertions.assertThatCode(() -> a.makeAssertion("desc", "fo foo")).doesNotThrowAnyException();
        Assertions.assertThatCode(() -> a.makeAssertion("desc", "fo bar")).doesNotThrowAnyException();
    }

    @Test
    void testDoesNotContain() {
        var a = new OutputFileAssertion(null, List.of(), List.of(), "foo");
        assertThatThrownBy(() -> a.makeAssertion("desc", "fo foo")).isExactlyInstanceOf(AssertionError.class);
        Assertions.assertThatCode(() -> a.makeAssertion("desc", "fo fo")).doesNotThrowAnyException();
    }

}
