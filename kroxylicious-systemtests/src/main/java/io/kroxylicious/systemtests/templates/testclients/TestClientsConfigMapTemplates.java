/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.testclients;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

/**
 * Templates for ConfigMaps consumed by test client jobs.
 */
public final class TestClientsConfigMapTemplates {

    private TestClientsConfigMapTemplates() {
    }

    /**
     * Builds a ConfigMap holding a jar file as binary data, keyed by the jar's file name, suitable for
     * mounting onto a test client's classpath with {@link TestClientsJobTemplates#withExtraClasspathJar}.
     * <p>
     * ConfigMaps are limited to 1MiB in total, so this is only suitable for small jars.
     *
     * @param namespace the namespace
     * @param name the ConfigMap name
     * @param jar path of the jar on the local filesystem
     * @return the ConfigMap builder
     */
    public static ConfigMapBuilder jarConfigMap(String namespace, String name, Path jar) {
        Path fileName = Objects.requireNonNull(jar.getFileName(), () -> "Not a file path: " + jar);
        byte[] content;
        try {
            content = Files.readAllBytes(jar);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to read jar " + jar, e);
        }
        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                .endMetadata()
                .withBinaryData(Map.of(fileName.toString(), Base64.getEncoder().encodeToString(content)));
        // @formatter:on
    }
}
