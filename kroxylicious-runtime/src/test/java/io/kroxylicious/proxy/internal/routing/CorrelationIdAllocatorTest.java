/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.routing;

import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CorrelationIdAllocatorTest {

    @Test
    public void correlationIdAllocatorStart() {
        CorrelationIdAllocator correlationIdAllocator = new CorrelationIdAllocator(0, 100, 0);
        int i = correlationIdAllocator.allocateId();
        assertThat(i).isEqualTo(0);
    }

    @Test
    public void correlationIdAllocatorIncrements() {
        CorrelationIdAllocator correlationIdAllocator = new CorrelationIdAllocator(0, 100, 0);
        correlationIdAllocator.allocateId();
        int i = correlationIdAllocator.allocateId();
        assertThat(i).isEqualTo(1);
    }

    @Test
    public void correlationIdAllocatorWraps() {
        CorrelationIdAllocator correlationIdAllocator = new CorrelationIdAllocator(0, 100, 99);
        int first = correlationIdAllocator.allocateId();
        assertThat(first).isEqualTo(99);
        int second = correlationIdAllocator.allocateId();
        assertThat(second).isEqualTo(0);
    }

    @Test
    public void correlationIdAllocatorIncrementsToMax() {
        CorrelationIdAllocator correlationIdAllocator = new CorrelationIdAllocator(0, 100, 98);
        int first = correlationIdAllocator.allocateId();
        assertThat(first).isEqualTo(98);
        int second = correlationIdAllocator.allocateId();
        assertThat(second).isEqualTo(99);
    }

    @Test
    public void correlationIdAllocatorHandlesNegatives() {
        CorrelationIdAllocator correlationIdAllocator = new CorrelationIdAllocator(
                Integer.MIN_VALUE, 0, Integer.MIN_VALUE);
        int first = correlationIdAllocator.allocateId();
        assertThat(first).isEqualTo(Integer.MIN_VALUE);
        int second = correlationIdAllocator.allocateId();
        assertThat(second).isEqualTo(Integer.MIN_VALUE + 1);
    }

    @Test
    public void correlationIdAllocatorWrapsNegatives() {
        CorrelationIdAllocator correlationIdAllocator = new CorrelationIdAllocator(Integer.MIN_VALUE,
                0, -1);
        int first = correlationIdAllocator.allocateId();
        assertThat(first).isEqualTo(-1);
        int second = correlationIdAllocator.allocateId();
        assertThat(second).isEqualTo(Integer.MIN_VALUE);
    }

    @Test
    void minEqualsMaxThrows() {
        // When / Then
        assertThatThrownBy(() -> new CorrelationIdAllocator(5, 5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void minExceedsMaxThrows() {
        // When / Then
        assertThatThrownBy(() -> new CorrelationIdAllocator(10, 5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void initialBelowMinThrows() {
        // When / Then
        assertThatThrownBy(() -> new CorrelationIdAllocator(5, 10, 4))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void initialAtMaxThrows() {
        // When / Then
        assertThatThrownBy(() -> new CorrelationIdAllocator(5, 10, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void twoArgConstructorStartsAtMin() {
        // Given
        CorrelationIdAllocator allocator = new CorrelationIdAllocator(7, 100);

        // When
        int first = allocator.allocateId();

        // Then
        assertThat(first).isEqualTo(7);
    }

    @Test
    void singleElementRangeWrapsImmediately() {
        // Given
        CorrelationIdAllocator allocator = new CorrelationIdAllocator(5, 6);

        // When
        int first = allocator.allocateId();
        int second = allocator.allocateId();

        // Then
        assertThat(first).isEqualTo(5);
        assertThat(second).isEqualTo(5);
    }

    @Test
    void nonZeroMinWrapsToMin() {
        // Given
        CorrelationIdAllocator allocator = new CorrelationIdAllocator(50, 52, 51);

        // When
        int atMax = allocator.allocateId();
        int wrapped = allocator.allocateId();

        // Then
        assertThat(atMax).isEqualTo(51);
        assertThat(wrapped).isEqualTo(50);
    }

    @Test
    void allocatesAllIdsInRangeBeforeWrapping() {
        // Given
        CorrelationIdAllocator allocator = new CorrelationIdAllocator(3, 6);

        // When
        var allocated = IntStream.range(0, 4).mapToObj(i -> allocator.allocateId()).toList();

        // Then
        assertThat(allocated).containsExactly(3, 4, 5, 3);
    }
}