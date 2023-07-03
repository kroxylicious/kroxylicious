/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

class BrokerAddressPatternUtils {
    public static final String LITERAL_NODE_ID = "$(nodeId)";
    public static final Pattern LITERAL_NODE_ID_PATTERN = Pattern.compile(Pattern.quote(LITERAL_NODE_ID));
    public static final Pattern PORT_SPECIFIER_RE = Pattern.compile(":([0-9]+)$");
    public static final Pattern TOKEN_RE = Pattern.compile("(\\$\\([^)]+\\))");

    static void validateTokens(String stringWithTokens,
                               Set<String> allowedTokens, Consumer<String> disallowedTokenFound,
                               Set<String> requiredTokens, Consumer<String> requiredTokenAbsent) {
        var tokenMatcher = TOKEN_RE.matcher(stringWithTokens);
        var requiredTokensCopy = new HashSet<>(requiredTokens == null ? Set.of() : requiredTokens);
        while (tokenMatcher.find()) {
            var token = tokenMatcher.group(1);
            if (!(allowedTokens.contains(token) || (requiredTokens != null && requiredTokens.contains(token)))) {
                disallowedTokenFound.accept(token);
            }
            requiredTokensCopy.remove(token);
        }
        if (!requiredTokensCopy.isEmpty() && requiredTokenAbsent != null) {
            requiredTokensCopy.forEach(requiredTokenAbsent);
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
}
