/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StableKroxyliciousLinkGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(StableKroxyliciousLinkGenerator.class);

    public static final StableKroxyliciousLinkGenerator INSTANCE = new StableKroxyliciousLinkGenerator();

    public static final String CLIENT_TLS = "clientTls";
    private final LinkInfo links;

    StableKroxyliciousLinkGenerator() {
        this(() -> {
            LOGGER.info("loading links from: classpath:META-INF/stablelinks.properties");
            return StableKroxyliciousLinkGenerator.class.getClassLoader().getResourceAsStream("META-INF/stablelinks.properties");
        });
    }

    StableKroxyliciousLinkGenerator(Supplier<InputStream> propLoader) {
        links = loadLinks(propLoader);
    }

    public String errorLink(String slug) {
        return links.generateLink("errors", slug);
    }

    private LinkInfo loadLinks(Supplier<InputStream> propLoader) {
        try (var resource = propLoader.get()) {
            if (resource != null) {
                Properties properties = new Properties();
                properties.load(resource);
                return new LinkInfo(properties);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new LinkInfo(Map.of());
    }

    private record LinkInfo(Map<String, String> properties) {
        LinkInfo(Properties properties) {
            this(properties.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString())));
        }

        public String generateLink(String namespace, String slug) {
            String lookupKey = "%s.%s".formatted(namespace, slug);
            if (properties.containsKey(lookupKey)) {
                return properties.get(lookupKey);
            }
            else {
                throw new IllegalArgumentException("No link found for " + lookupKey);
            }
        }
    }
}