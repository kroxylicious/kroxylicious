/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.SecretBuilder;

/**
 * The type Kroxylicious secret templates.
 */
public class KroxyliciousSecretTemplates {

    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousSecretTemplates.class);

    private KroxyliciousSecretTemplates() {
    }

    /**
     * Create registry credentials secret.
     *
     * @param configFilePath the config file path
     * @param namespace the namespace
     * @return the secret builder
     */
    public static SecretBuilder createRegistryCredentialsSecret(String configFilePath, String namespace) {
        if (configFilePath != null) {
            Path configPath = Path.of(configFilePath);
            if (Files.exists(configPath)) {
                LOGGER.info("{} config file found. Creating the secret within kubernetes {} namespace", configFilePath, namespace);
                try {
                    String encoded = Base64.getEncoder().encodeToString(Files.readAllBytes(configPath));
                    return new SecretBuilder()
                                              .withNewMetadata()
                                              .withName("regcred")
                                              .withNamespace(namespace)
                                              .endMetadata()
                                              .withType("kubernetes.io/dockerconfigjson")
                                              .addToData(".dockerconfigjson", encoded);
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Failed to create secret for file " + configPath, e);
                }
            }
        }
        return null;
    }
}
