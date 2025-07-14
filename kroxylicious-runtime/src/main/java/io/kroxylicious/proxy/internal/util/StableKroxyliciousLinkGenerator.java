/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import io.kroxylicious.proxy.VersionInfo;

public class StableKroxyliciousLinkGenerator {

    public static String errorLink(String slug) {
        return generateLink("errors", slug);
    }

    public static String generateLink(String namespace, String slug) {
        return String.format("https://kroxylicious.io/redirects/%s/%s/%s", namespace, VersionInfo.VERSION_INFO.version(), slug);
    }
}
