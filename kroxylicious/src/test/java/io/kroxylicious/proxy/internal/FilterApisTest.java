/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.ApiVersions;
import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.OffsetCommitRequestFilter;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterApisTest {

    static class MyFilter implements
            ProduceRequestFilter,
            FetchResponseFilter,
            MetadataResponseFilter,
            OffsetCommitRequestFilter {

        @Override
        public void onProduceRequest(ProduceRequestData request, KrpcFilterContext context) {

        }

        @ApiVersions(from = 5)
        @Override
        public void onFetchResponse(FetchResponseData response, KrpcFilterContext context) {

        }

        @ApiVersions(to = 7)
        @Override
        public void onMetadataResponse(MetadataResponseData response,
                                       KrpcFilterContext context) {

        }

        @ApiVersions(from = 2, to = 6)
        @Override
        public void onOffsetCommitRequest(OffsetCommitRequestData request,
                                          KrpcFilterContext context) {

        }
    }

    @Test
    public void testToString() {

        var bitSet = FilterApis.forFilter(MyFilter.class);
        assertEquals("PRODUCE_REQUESTv0-9," +
                "FETCH_RESPONSEv5-13," +
                "METADATA_RESPONSEv0-7," +
                "OFFSET_COMMIT_REQUESTv2-6", bitSet.toString());
    }

    @Test
    public void consumesAnyVersion() {
        var bitSet = FilterApis.forFilter(MyFilter.class);
        assertTrue(bitSet.consumesAnyVersion(FilterType.PRODUCE_REQUEST));
        assertTrue(bitSet.consumesAnyVersion(FilterType.FETCH_RESPONSE));
        assertTrue(bitSet.consumesAnyVersion(FilterType.METADATA_RESPONSE));
        assertTrue(bitSet.consumesAnyVersion(FilterType.OFFSET_COMMIT_REQUEST));

        var expectAbsent = EnumSet.complementOf(EnumSet.of(
                FilterType.PRODUCE_REQUEST,
                FilterType.FETCH_RESPONSE,
                FilterType.METADATA_RESPONSE,
                FilterType.OFFSET_COMMIT_REQUEST));
        var unexpectedlyPresent = new HashSet<String>();
        for (var ft : expectAbsent) {
            if (bitSet.consumesAnyVersion(ft)) {
                unexpectedlyPresent.add(ft.toString());
            }
        }
        assertEquals(Set.of(), unexpectedlyPresent, "bitset contains unexpected APIs");
    }

    @Test
    public void consumesApiVersion() {

        var bitSet = FilterApis.forFilter(MyFilter.class);

        // No @ApiVersions
        for (short version = ApiKeys.PRODUCE.oldestVersion(); version <= ApiKeys.PRODUCE.latestVersion(); version++) {
            assertTrue(bitSet.consumesApiVersion(FilterType.PRODUCE_REQUEST, version),
                    "Expect interest in all versions of PRODUCE request, but no interest in " + version);
            assertFalse(bitSet.consumesApiVersion(FilterType.PRODUCE_RESPONSE, version),
                    "Don't expect interest in any version of PRODUCE response, but interest in " + version);
        }

        // Lower bounded @ApiVersions
        for (short version = ApiKeys.FETCH.oldestVersion(); version <= ApiKeys.FETCH.latestVersion(); version++) {
            assertFalse(bitSet.consumesApiVersion(FilterType.FETCH_REQUEST, version),
                    "Don't expect interest in any version of FETCH request, but interest in " + version);
            if (version >= 5) {
                assertTrue(bitSet.consumesApiVersion(FilterType.FETCH_RESPONSE, version),
                        "Expect interest in versions >= 5 of FETCH response, but no interest in " + version);
            }
            else {
                assertFalse(bitSet.consumesApiVersion(FilterType.FETCH_RESPONSE, version),
                        "Don't expect interest in versions < 5 of FETCH response, but interest in " + version);
            }
        }

        // Upper bounded @ApiVersions
        for (short version = ApiKeys.METADATA.oldestVersion(); version <= ApiKeys.METADATA.latestVersion(); version++) {
            assertFalse(bitSet.consumesApiVersion(FilterType.METADATA_REQUEST, version),
                    "Don't expect interest in any version of METADATA request, but interest in " + version);
            if (version <= 7) {
                assertTrue(bitSet.consumesApiVersion(FilterType.METADATA_RESPONSE, version),
                        "Expect interest in versions <= 7 of METADATA response, but no interest in " + version);
            }
            else {
                assertFalse(bitSet.consumesApiVersion(FilterType.METADATA_RESPONSE, version),
                        "Don't expect interest in versions > 7 of METADATA response, but interest in " + version);
            }
        }

        // Bracketed bounded @ApiVersions
        for (short version = ApiKeys.OFFSET_COMMIT.oldestVersion(); version <= ApiKeys.OFFSET_COMMIT.latestVersion(); version++) {
            assertFalse(bitSet.consumesApiVersion(FilterType.OFFSET_COMMIT_RESPONSE, version),
                    "Don't expect interest in any version of OFFSET_COMMIT response, but interest in " + version);
            if (2 <= version && version <= 6) {
                assertTrue(bitSet.consumesApiVersion(FilterType.OFFSET_COMMIT_REQUEST, version),
                        "Expect interest in versions in [2, 6] of OFFSET_COMMIT request, but no interest in " + version);

            }
            else {
                assertFalse(bitSet.consumesApiVersion(FilterType.OFFSET_COMMIT_REQUEST, version),
                        "Don't expect interest in versions not in [2, 6] of OFFSET_COMMIT request, but interest in " + version);
            }
        }

        var expectAbsent = EnumSet.complementOf(EnumSet.of(
                FilterType.PRODUCE_REQUEST,
                FilterType.FETCH_RESPONSE,
                FilterType.METADATA_RESPONSE,
                FilterType.OFFSET_COMMIT_REQUEST));
        var unexpectedlyPresent = new HashSet<String>();
        for (var ft : expectAbsent) {
            for (short version = ft.messageType.lowestSupportedVersion(); version <= ft.messageType.highestSupportedVersion(); version++) {
                if (bitSet.consumesApiVersion(ft, version)) {
                    unexpectedlyPresent.add(ft.apiKey + (ft.isRequest() ? "request" : "response") + " v" + version);
                }
            }
        }
        assertEquals(Set.of(), unexpectedlyPresent, "bitset contains unexpected APIs");
    }

    static class BadVersionsFilter implements FetchResponseFilter {

        @ApiVersions(from = 1000)
        @Override
        public void onFetchResponse(FetchResponseData response, KrpcFilterContext context) {

        }

    }

    @Test
    public void badVersions() {
        var iae = assertThrows(IllegalArgumentException.class,
                () -> FilterApis.forFilter(BadVersionsFilter.class));
        assertEquals("@io.kroxylicious.proxy.filter.ApiVersions(from=1000, to=-1) " +
                "on/inherited by class io.kroxylicious.proxy.internal.FilterApisTest$BadVersionsFilter " +
                "has 'from' outside the range [0, 13] defined for the FETCH response API",
                iae.getMessage());
    }

    static class SwappedBoundsFilter implements FetchResponseFilter {

        @ApiVersions(from = 5, to = 3)
        @Override
        public void onFetchResponse(FetchResponseData response, KrpcFilterContext context) {

        }

    }

    @Test
    public void swappedBounds() {
        var iae = assertThrows(IllegalArgumentException.class,
                () -> FilterApis.forFilter(SwappedBoundsFilter.class));
        assertEquals("@io.kroxylicious.proxy.filter.ApiVersions(from=5, to=3) " +
                "on/inherited by class io.kroxylicious.proxy.internal.FilterApisTest$SwappedBoundsFilter " +
                "has 'from' > 'to'",
                iae.getMessage());
    }

}
