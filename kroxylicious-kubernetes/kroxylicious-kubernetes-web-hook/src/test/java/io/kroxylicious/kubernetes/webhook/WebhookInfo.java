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
 * Provides webhook image metadata, read from a filtered properties file.
 */
record WebhookInfo(String imageName,
                   String imageArchive) {

    static WebhookInfo fromResource() {
        try (var is = WebhookInfo.class.getResourceAsStream("/webhook-info.properties")) {
            var properties = new Properties();
            properties.load(is);
            String imageName = properties.getProperty("webhook.image.name");
            String imageArchive = properties.getProperty("webhook.image.archive");
            return new WebhookInfo(imageName, imageArchive);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
