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

/**
 * Generates stable links to Kroxylicious documentation, resolving well-known slugs to URLs via the
 * {@code META-INF/stablelinks.properties} classpath resource. Such links are suitable for inclusion
 * in log or error messages as they remain valid across releases.
 */
public class StableKroxyliciousLinkGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(StableKroxyliciousLinkGenerator.class);

    /** Shared instance backed by the {@code META-INF/stablelinks.properties} classpath resource. */
    public static final StableKroxyliciousLinkGenerator INSTANCE = new StableKroxyliciousLinkGenerator();

    /** Slug for the client TLS documentation link. */
    public static final String CLIENT_TLS = "clientTls";
    private final LinkInfo links;

    StableKroxyliciousLinkGenerator() {
        this(() -> {
            LOGGER.atInfo()
                    .log("Loading links from: classpath:META-INF/stablelinks.properties");
            return StableKroxyliciousLinkGenerator.class.getClassLoader().getResourceAsStream("META-INF/stablelinks.properties");
        });
    }

    StableKroxyliciousLinkGenerator(Supplier<InputStream> propLoader) {
        links = loadLinks(propLoader);
    }

    /**
     * Resolves a slug from the {@code errors} namespace to its documentation URL.
     *
     * @param slug the well-known identifier of the error documentation
     * @return the URL for the error documentation
     * @throws IllegalArgumentException if no link is known for the slug
     */
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

        private String generateLink(String namespace, String slug) {
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