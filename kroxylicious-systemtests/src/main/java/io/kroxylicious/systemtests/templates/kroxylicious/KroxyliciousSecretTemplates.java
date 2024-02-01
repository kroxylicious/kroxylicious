/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import io.fabric8.kubernetes.api.model.SecretBuilder;

/**
 * The type Kroxylicious secret templates.
 */
public class KroxyliciousSecretTemplates {

    /**
     * Create registry credentials secret.
     *
     * @param configFolder the config folder
     * @param namespace the namespace
     * @return the secret builder
     */
    public static SecretBuilder createRegistryCredentialsSecret(String configFolder, String namespace) {
        if (configFolder != null) {
            Path configPath = Path.of(configFolder);
            if (Files.exists(configPath)) {
                String encoded;
                try {
                    encoded = Base64.getEncoder().encodeToString(Files.readAllBytes(configPath));
                    return new SecretBuilder()
                            .withNewMetadata()
                            .withName("regcred")
                            .withNamespace(namespace)
                            .endMetadata()
                            .withType("kubernetes.io/dockerconfigjson")
                            .addToData(".dockerconfigjson", encoded);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }
}
