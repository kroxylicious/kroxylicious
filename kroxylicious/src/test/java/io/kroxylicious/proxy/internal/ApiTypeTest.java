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

class ApiTypeTest {

    @Test
    void isRequest() {
        assertTrue(ApiType.LIST_OFFSETS_REQUEST.isRequest());
        assertTrue(ApiType.PRODUCE_REQUEST.isRequest());
        assertFalse(ApiType.LIST_OFFSETS_REQUEST.isResponse());
        assertFalse(ApiType.PRODUCE_REQUEST.isResponse());

        assertFalse(ApiType.LIST_OFFSETS_RESPONSE.isRequest());
        assertFalse(ApiType.PRODUCE_RESPONSE.isRequest());
        assertTrue(ApiType.LIST_OFFSETS_RESPONSE.isResponse());
        assertTrue(ApiType.PRODUCE_RESPONSE.isResponse());
    }

    @Test
    void index() {
        var requestIndices = new TreeSet<Integer>();
        for (short version = ApiType.LIST_OFFSETS_REQUEST.messageType.lowestSupportedVersion(); version <= ApiType.LIST_OFFSETS_REQUEST.messageType
                .highestSupportedVersion(); version++) {
            int index = ApiType.LIST_OFFSETS_REQUEST.index(version);
            if (!requestIndices.isEmpty()) {
                assertEquals(requestIndices.last() + 1, index);
            }
            requestIndices.add(index);
        }
        assertEquals(requestIndices.size(),
                ApiType.LIST_OFFSETS_REQUEST.messageType.highestSupportedVersion()
                        - ApiType.LIST_OFFSETS_REQUEST.messageType.lowestSupportedVersion() + 1);

        var responseIndices = new TreeSet<Integer>();
        for (short version = ApiType.LIST_OFFSETS_RESPONSE.messageType.lowestSupportedVersion(); version <= ApiType.LIST_OFFSETS_RESPONSE.messageType
                .highestSupportedVersion(); version++) {
            int index = ApiType.LIST_OFFSETS_RESPONSE.index(version);
            if (!responseIndices.isEmpty()) {
                assertEquals(responseIndices.last() + 1, index);
            }
            responseIndices.add(index);
        }

        assertEquals(responseIndices.size(),
                ApiType.LIST_OFFSETS_RESPONSE.messageType.highestSupportedVersion()
                        - ApiType.LIST_OFFSETS_RESPONSE.messageType.lowestSupportedVersion() + 1);

        var intersection = new HashSet<>(requestIndices);
        intersection.retainAll(responseIndices);

        assertEquals(Set.of(), intersection,
                "Expect disjoint indices");

        assertEquals(requestIndices.last() + 1, responseIndices.first(),
                "Expect _REQUEST and _RESPONSE to be contiguous");
    }
}