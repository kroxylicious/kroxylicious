/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;

import io.kroxylicious.proxy.filter.ApiVersions;
import io.kroxylicious.proxy.filter.KrpcFilter;

public class FilterApis {

    private static LinkedHashMap<Class<? extends KrpcFilter>, FilterApis> CLASS_CACHE = new LinkedHashMap<>(16, 0.75f, true) {
        private static final int MAX_ENTRIES = 32;

        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > MAX_ENTRIES;
        }
    };
    private final BitSet bitSet;

    private FilterApis(BitSet bitSet) {
        this.bitSet = bitSet;
    }

    public static FilterApis forFilter(Class<? extends KrpcFilter> filterClass) {
        return CLASS_CACHE.computeIfAbsent(filterClass, k -> {
            var bitSet = bitset(filterClass);
            return new FilterApis(bitSet);
        });
    }

    public static FilterApis empty() {
        return new FilterApis(new BitSet(ApiType.NUM_APIS));
    }

    private static BitSet bitset(Class<? extends KrpcFilter> filterClass) {
        if (filterClass == KrpcFilter.class) {
            throw new IllegalArgumentException();
        }
        var bitSet = new BitSet(ApiType.NUM_APIS);
        // loop over filters
        for (var filterType : ApiType.values()) {
            if (filterType.filterClass.isAssignableFrom(filterClass)) {
                var versionsAnno = filterType.annotations(ApiVersions.class, filterClass);
                int from = filterType.messageType.lowestSupportedVersion();
                int to = filterType.messageType.highestSupportedVersion();
                if (versionsAnno != null && versionsAnno.from() >= 0) {
                    checkVersionWithinApiBounds(versionsAnno.from(), "from", filterClass, filterType, versionsAnno);
                    from = versionsAnno.from();
                }
                if (versionsAnno != null && versionsAnno.to() >= 0) {
                    checkVersionWithinApiBounds(versionsAnno.to(), "to", filterClass, filterType, versionsAnno);
                    to = versionsAnno.to();
                }
                if (from > to) {
                    throw new IllegalArgumentException(String.format(
                            "%s on/inherited by %s has 'from' > 'to'",
                            versionsAnno, filterClass));
                }
                for (int version = from; version <= to; version++) {
                    bitSet.set(filterType.index(version));
                }
            }
        }
        return bitSet;
    }

    private static void checkVersionWithinApiBounds(
                                                    int v, String vName,
                                                    Class<? extends KrpcFilter> filterClass, ApiType filterType, ApiVersions versions) {
        if (v > filterType.messageType.highestSupportedVersion()
                || v < filterType.messageType.lowestSupportedVersion()) {
            throw new IllegalArgumentException(String.format(
                    "%s on/inherited by %s has '%s' outside the range [%d, %d] defined for the %s %s API",
                    versions, filterClass, vName,
                    filterType.messageType.lowestSupportedVersion(),
                    filterType.messageType.highestSupportedVersion(),
                    filterType.apiKey,
                    (filterType.isRequest() ? "request" : "response")));
        }
    }

    /**
     * @param type A filter type
     * @param apiVersion The API version
     * @return Whether the given {@code filter} consumes requests or responses of the given {@code apiKey}.
     */
    public boolean consumesApiVersion(ApiType type, short apiVersion) {
        return bitSet.get(type.index(apiVersion));
    }

    public boolean consumesAnyVersion(ApiType type) {
        for (int index = type.highestIndex(); index >= type.lowestIndex(); index--) {
            if (bitSet.get(index)) {
                return true;
            }
        }
        return false;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean appendedAny = false;
        for (var filterType : ApiType.values()) {
            short from = filterType.messageType.lowestSupportedVersion();
            short to = filterType.messageType.highestSupportedVersion();
            for (short version = from; version <= to; version++) {
                if (bitSet.get(filterType.index(version))) {
                    if (appendedAny) {
                        sb.append(',');
                    }
                    short startVersion = version;
                    sb.append(filterType.name()).append('v').append(version);
                    appendedAny = true;
                    for (; version <= to; version++) {
                        if (!bitSet.get(filterType.index(version)))
                            break;
                    }
                    if (startVersion == version - 1) {
                    }
                    else {
                        sb.append('-').append(version - 1);
                    }

                }
            }
        }
        return sb.toString();
    }

    public void orInplace(FilterApis forFilter) {
        bitSet.or(forFilter.bitSet);
    }
}
