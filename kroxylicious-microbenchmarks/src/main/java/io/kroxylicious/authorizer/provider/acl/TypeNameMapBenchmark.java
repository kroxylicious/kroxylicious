/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares {@link TypeNameMap#matchingValues(OrderedKey)} against the algorithm it replaced
 * (as it existed at commit {@code 8b13c0625}), to check the claim made in the "Algorithmic
 * improvement" commit ({@code 2f5ae120a}) that a {@code subMap} range-scan beats the original
 * per-prefix-length {@code TreeMap.get()} backward scan. That commit message says outright
 * "I've not proven that this is actually a performance improvement in practice" - this
 * benchmark exists to answer that question.
 * <p>This class lives in {@code io.kroxylicious.authorizer.provider.acl} (a split package
 * across this module and {@code kroxylicious-authorizer-acl} - there's no JPMS module system
 * in this repo, so that's fine) purely to reach {@link TypeNameMap} and its package-private key
 * types without widening their visibility just for a benchmark.
 */
@Fork(value = 2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class TypeNameMapBenchmark {

    private static final char[] ALPHABET = "abcdefgh".toCharArray();

    @State(Scope.Benchmark)
    public static class MapState {

        @Param({ "16", "256", "1024" })
        int ruleCount;

        @Param({ "16", "32", "64", "128" })
        int nameLength;

        TypeNameMap<String, Integer> current;
        LegacyTypeNameMap<String, Integer> legacy;
        ResourceMatcherNameStarts<String> lookupKey;

        @Setup(Level.Trial)
        public void setup() {
            Random random = new Random(31L * ruleCount + nameLength);
            current = new TypeNameMap<>();
            legacy = new LegacyTypeNameMap<>();

            String lookupName = randomString(random, nameLength);
            lookupKey = new ResourceMatcherNameStarts<>(String.class, lookupName);

            Set<String> usedPrefixes = new HashSet<>();
            int value = 0;
            // A chain of overlapping prefix rules, every one a prefix of lookupName: mirrors a
            // real ACL resource hierarchy where several prefix rules can all apply at once (see
            // TypeNameMap's own javadoc on matchingValues).
            for (int len = 1; len <= nameLength; len *= 2) {
                String prefix = lookupName.substring(0, len);
                usedPrefixes.add(prefix);
                addRule(prefix, value++);
            }
            // Pad the map out to ruleCount with unrelated prefixes, so the TreeMap is
            // realistically sized and the subMap scan has real (mostly non-matching)
            // candidates to filter via startsWith.
            while (usedPrefixes.size() < ruleCount) {
                String prefix = randomString(random, random.nextInt(nameLength + 1));
                if (usedPrefixes.add(prefix)) {
                    addRule(prefix, value++);
                }
            }
        }

        private void addRule(String prefix, int value) {
            current.addApply(new ResourceMatcherNameStarts<>(String.class, prefix), v -> value);
            legacy.addApply(new ResourceMatcherNameStarts<>(String.class, prefix), v -> value);
        }

        private static String randomString(Random random, int length) {
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = ALPHABET[random.nextInt(ALPHABET.length)];
            }
            return new String(chars);
        }
    }

    @Benchmark
    public List<Integer> currentAlgorithm(MapState state) {
        return state.current.matchingValues(state.lookupKey);
    }

    @Benchmark
    public List<Integer> legacyAlgorithm(MapState state) {
        return state.legacy.matchingValues(state.lookupKey);
    }

    /**
     * Reproduction of {@link TypeNameMap} as it existed at commit {@code 8b13c0625}, before the
     * {@code subMap}-based algorithm. Reuses the current {@link OrderedKey} key types unchanged:
     * the comparator fix in {@code c0181b6a3} only reordered tie-breaks between keys that compare
     * unequal, which this algorithm - relying only on {@code compareTo() == 0} to find an exact
     * match - never depended on.
     */
    static class LegacyTypeNameMap<T, V> {

        private final TreeMap<OrderedKey<T>, V> map = new TreeMap<>();

        V addApply(OrderedKey<T> key, UnaryOperator<V> value) {
            return map.compute(key, (k, v) -> value.apply(v));
        }

        List<V> matchingValues(OrderedKey<T> lookupable) {
            if (lookupable instanceof ResourceMatcherNameEquals || lookupable instanceof ResourceMatcherAnyOfType<T>) {
                V value = map.get(lookupable);
                return value == null ? List.of() : List.of(value);
            }
            else if (lookupable instanceof ResourceMatcherNameStarts<T> starts) {
                // Each prefix of the name, longest first, looked up as an exact key: of the same
                // type by construction, and every overlapping prefix rule is found
                String name = starts.prefix();
                List<V> result = new ArrayList<>();
                for (int length = name.length(); length >= 0; length--) {
                    V value = map.get(new ResourceMatcherNameStarts<T>(starts.type(), name.substring(0, length)));
                    if (value != null) {
                        result.add(value);
                    }
                }
                return result;
            }
            throw new IllegalArgumentException("Unknown lookupable: " + lookupable);
        }
    }
}
