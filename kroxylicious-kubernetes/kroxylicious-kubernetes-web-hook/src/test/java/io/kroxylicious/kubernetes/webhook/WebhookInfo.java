/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

/**
 * Provides webhook and related image metadata, read from a filtered properties file.
 */
record WebhookInfo(String imageName,
                   String imageArchive,
                   String proxyImageName,
                   String proxyImageArchive,
                   String testPluginImageName,
                   String testPluginImageArchive) {

    /** Alias for {@link #imageArchive()} — webhook image archive. */
    String webhookImageArchive() {
        return imageArchive;
    }

    static WebhookInfo fromResource() {
        try (var is = WebhookInfo.class.getResourceAsStream("/webhook-info.properties")) {
            var properties = new Properties();
            properties.load(is);
            return new WebhookInfo(
                    properties.getProperty("webhook.image.name"),
                    properties.getProperty("webhook.image.archive"),
                    properties.getProperty("proxy.image.name"),
                    properties.getProperty("proxy.image.archive"),
                    properties.getProperty("test.plugin.image.name"),
                    properties.getProperty("test.plugin.image.archive"));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
