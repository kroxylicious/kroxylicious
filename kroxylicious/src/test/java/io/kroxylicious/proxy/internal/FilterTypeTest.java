/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterTypeTest {

    @Test
    void isRequest() {
        assertTrue(FilterType.LIST_OFFSETS_REQUEST.isRequest());
        assertTrue(FilterType.PRODUCE_REQUEST.isRequest());
        assertFalse(FilterType.LIST_OFFSETS_REQUEST.isResponse());
        assertFalse(FilterType.PRODUCE_REQUEST.isResponse());

        assertFalse(FilterType.LIST_OFFSETS_RESPONSE.isRequest());
        assertFalse(FilterType.PRODUCE_RESPONSE.isRequest());
        assertTrue(FilterType.LIST_OFFSETS_RESPONSE.isResponse());
        assertTrue(FilterType.PRODUCE_RESPONSE.isResponse());
    }

    @Test
    void index() {
        var requestIndices = new TreeSet<Integer>();
        for (short version = FilterType.LIST_OFFSETS_REQUEST.messageType.lowestSupportedVersion(); version <= FilterType.LIST_OFFSETS_REQUEST.messageType
                .highestSupportedVersion(); version++) {
            int index = FilterType.LIST_OFFSETS_REQUEST.index(version);
            if (!requestIndices.isEmpty()) {
                assertEquals(requestIndices.last() + 1, index);
            }
            requestIndices.add(index);
        }
        assertEquals(requestIndices.size(),
                FilterType.LIST_OFFSETS_REQUEST.messageType.highestSupportedVersion()
                        - FilterType.LIST_OFFSETS_REQUEST.messageType.lowestSupportedVersion() + 1);

        var responseIndices = new TreeSet<Integer>();
        for (short version = FilterType.LIST_OFFSETS_RESPONSE.messageType.lowestSupportedVersion(); version <= FilterType.LIST_OFFSETS_RESPONSE.messageType
                .highestSupportedVersion(); version++) {
            int index = FilterType.LIST_OFFSETS_RESPONSE.index(version);
            if (!responseIndices.isEmpty()) {
                assertEquals(responseIndices.last() + 1, index);
            }
            responseIndices.add(index);
        }

        assertEquals(responseIndices.size(),
                FilterType.LIST_OFFSETS_RESPONSE.messageType.highestSupportedVersion()
                        - FilterType.LIST_OFFSETS_RESPONSE.messageType.lowestSupportedVersion() + 1);

        var intersection = new HashSet<>(requestIndices);
        intersection.retainAll(responseIndices);

        assertEquals(Set.of(), intersection,
                "Expect disjoint indices");

        assertEquals(requestIndices.last() + 1, responseIndices.first(),
                "Expect _REQUEST and _RESPONSE to be contiguous");
    }
}