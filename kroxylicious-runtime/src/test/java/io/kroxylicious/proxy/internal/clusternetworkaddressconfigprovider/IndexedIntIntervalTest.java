/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import org.junit.jupiter.api.Test;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.IndexedIntInterval.INTERVAL_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexedIntIntervalTest {

    @Test
    void invalidInputs() {
        assertDoesNotMatchIntervalPattern("z1,3]");
        assertDoesNotMatchIntervalPattern("[a,3]");
        assertDoesNotMatchIntervalPattern("[0|3]");
        assertDoesNotMatchIntervalPattern("[1,a]");
        assertDoesNotMatchIntervalPattern("[1,3.");
    }

    @Test
    void bothInclusive() {
        IndexedIntSet intInterval = IndexedIntSet.parseInterval("[1,3]");
        int[] array = intInterval.values().stream().mapToInt(value -> value).toArray();
        assertThat(array).containsExactly(1, 2, 3);
    }

    @Test
    void endMustBeAfterStart() {
        assertThatThrownBy(() -> IndexedIntInterval.parse("[1,1)")).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("endExclusive 1 less than or equal to startInclusive 1");
        assertThatThrownBy(() -> IndexedIntInterval.parse("[1,0)")).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("endExclusive 0 less than or equal to startInclusive 1");
    }

    @Test
    void bothExclusive() {
        IndexedIntSet intInterval = IndexedIntSet.parseInterval("(1,4)");
        int[] array = intInterval.values().stream().mapToInt(value -> value).toArray();
        assertThat(array).containsExactly(2, 3);
    }

    @Test
    void exclusiveStartInclusiveEnd() {
        IndexedIntSet intInterval = IndexedIntSet.parseInterval("(1,3]");
        int[] array = intInterval.values().stream().mapToInt(value -> value).toArray();
        assertThat(array).containsExactly(2, 3);
    }

    @Test
    void inclusiveStartExclusiveEnd() {
        IndexedIntSet intInterval = IndexedIntSet.parseInterval("[0,3)");
        int[] array = intInterval.values().stream().mapToInt(value -> value).toArray();
        assertThat(array).containsExactly(0, 1, 2);
    }

    @Test
    void testContains() {
        IndexedIntSet intInterval = IndexedIntSet.parseInterval("[1,2]");
        assertThat(intInterval.contains(-1)).isFalse();
        assertThat(intInterval.contains(0)).isFalse();
        assertThat(intInterval.contains(1)).isTrue();
        assertThat(intInterval.contains(2)).isTrue();
        assertThat(intInterval.contains(3)).isFalse();
    }

    @Test
    void testSize() {
        IndexedIntSet intInterval = IndexedIntSet.parseInterval("[1,2]");
        assertThat(intInterval.size()).isEqualTo(2);
    }

    @Test
    void testIndexOf() {
        IndexedIntSet intInterval = IndexedIntSet.parseInterval("[1,2]");
        assertThat(intInterval.indexOf(0)).isEqualTo(-1);
        assertThat(intInterval.indexOf(1)).isEqualTo(0);
        assertThat(intInterval.indexOf(2)).isEqualTo(1);
        assertThat(intInterval.indexOf(3)).isEqualTo(-1);
    }

    private static void assertDoesNotMatchIntervalPattern(String input) {
        assertThatThrownBy(() -> IndexedIntSet.parseInterval(input)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("range string " + input + " did not match " + INTERVAL_PATTERN.pattern());
    }
}