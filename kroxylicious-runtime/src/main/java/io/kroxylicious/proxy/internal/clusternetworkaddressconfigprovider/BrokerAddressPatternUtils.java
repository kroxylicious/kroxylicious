/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

final class BrokerAddressPatternUtils {
    private static final String LITERAL_NODE_ID = "$(nodeId)";
    private static final Pattern LITERAL_NODE_ID_PATTERN = Pattern.compile(Pattern.quote(LITERAL_NODE_ID));
    private static final Pattern PORT_SPECIFIER_RE = Pattern.compile(":([0-9]+)$");
    private static final Pattern TOKEN_RE = Pattern.compile("(\\$\\([^)]+\\))");
    public static Set<String> EXPECTED_TOKEN_SET = Set.of(LITERAL_NODE_ID);

    private BrokerAddressPatternUtils() {
    }

    private static Set<String> extractTokens(String stringWithTokens) {
        var tokenMatcher = TOKEN_RE.matcher(stringWithTokens);
        var tokens = new TreeSet<String>();
        while (tokenMatcher.find()) {
            var token = tokenMatcher.group(1);
            tokens.add(token);
        }
        return tokens;

    }

    static void validateStringContainsOnlyExpectedTokens(
            String stringWithPossibleTokens,
            Set<String> allowedTokens,
            Consumer<String> disallowedTokenConsumer
    ) {
        Objects.requireNonNull(stringWithPossibleTokens);
        Objects.requireNonNull(allowedTokens);
        Objects.requireNonNull(disallowedTokenConsumer);
        var tokens = extractTokens(stringWithPossibleTokens);
        tokens.removeAll(allowedTokens);
        tokens.forEach(disallowedTokenConsumer);
    }

    static void validateStringContainsRequiredTokens(
            String stringWithTokens,
            Set<String> requiredTokens,
            Consumer<String> requiredTokenAbsentConsumer
    ) {
        Objects.requireNonNull(stringWithTokens);
        Objects.requireNonNull(requiredTokens);
        Objects.requireNonNull(requiredTokenAbsentConsumer);

        var tokens = extractTokens(stringWithTokens);
        var requiredTokensCopy = new TreeSet<>(requiredTokens);
        requiredTokensCopy.removeAll(tokens);

        if (!requiredTokensCopy.isEmpty()) {
            requiredTokensCopy.forEach(requiredTokenAbsentConsumer);
        }
    }

    static void validatePortSpecifier(String address, Consumer<String> portNumber) {
        var portMatcher = PORT_SPECIFIER_RE.matcher(address);
        if (portMatcher.find()) {
            portNumber.accept(portMatcher.group(1));
        }
    }

    static String replaceLiteralNodeId(String brokerAddressPattern, int nodeId) {
        return LITERAL_NODE_ID_PATTERN.matcher(brokerAddressPattern).replaceAll(Integer.toString(nodeId));
    }

    static Pattern createNodeIdCapturingRegex(String brokerAddressPattern) {
        var partsQuoted = Arrays.stream(brokerAddressPattern.split(Pattern.quote(LITERAL_NODE_ID))).map(Pattern::quote);
        return Pattern.compile(partsQuoted.collect(Collectors.joining("(\\d+)")), Pattern.CASE_INSENSITIVE);
    }
}
