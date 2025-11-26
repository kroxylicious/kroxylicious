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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.SecretBuilder;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

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

    /**
     * Create certificate secret builder.
     *
     * @param secretName the secret name
     * @param namespace the namespace
     * @param certificateName the certificate name
     * @param certificatePath the certificate path
     * @return  the secret builder
     */
    public static SecretBuilder createCertificateSecret(String secretName, String namespace, String certificateName, String certificatePath, String password) {
        String encoded;
        try {
            encoded = Base64.getEncoder().encodeToString(Files.readAllBytes(Path.of(certificatePath)));
        }
        catch (IOException e) {
            throw new KubeClusterException(e);
        }
        // @formatter:off
        return new SecretBuilder()
                .withKind(Constants.SECRET)
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(namespace)
                .endMetadata()
                .withData(Map.of(certificateName, encoded, "password", password));
        // @formatter:on
    }

    /**
     * Create password secret builder.
     *
     * @param namespace the namespace
     * @param secretName the secret name
     * @param secretPassword the secret password
     * @return  the secret builder
     */
    public static SecretBuilder createPasswordSecret(String namespace, String secretName, String secretPassword) {
        // @formatter:off
        return new SecretBuilder()
                .withKind(Constants.SECRET)
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(namespace)
                .endMetadata()
                .withStringData(Map.of("password", secretPassword));
        // @formatter:on
    }
}
