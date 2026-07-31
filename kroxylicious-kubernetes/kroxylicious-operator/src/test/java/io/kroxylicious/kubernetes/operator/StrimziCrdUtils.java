/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.InputStream;
import java.util.List;
import java.util.function.Predicate;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.strimzi.api.kafka.model.kafka.Kafka;

public final class StrimziCrdUtils {

    private StrimziCrdUtils() {
    }

    public static List<CustomResourceDefinition> crds(Predicate<CustomResourceDefinition> filter) {
        String version = Kafka.class.getPackage().getImplementationVersion();
        if (version == null) {
            throw new IllegalStateException(
                    "Could not determine Strimzi API version from the JAR manifest. "
                            + "Ensure io.strimzi:api is on the classpath as a JAR (not exploded classes).");
        }
        String resourcePath = "strimzi-crds/" + version + "/strimzi-crds.yaml";
        try (InputStream is = StrimziCrdUtils.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException(
                        "No Strimzi CRDs found for Strimzi API version " + version + ". "
                                + "Fetch strimzi-crds-" + version + ".yaml from the Strimzi " + version + " release "
                                + "(https://github.com/strimzi/strimzi-kafka-operator/releases/tag/" + version + ") "
                                + "and place it at src/test/resources/" + resourcePath);
            }
            List<HasMetadata> resources = Serialization.unmarshal(is);
            return resources.stream()
                    .filter(CustomResourceDefinition.class::isInstance)
                    .map(CustomResourceDefinition.class::cast)
                    .filter(filter)
                    .toList();
        }
        catch (IllegalStateException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to load Strimzi CRDs from " + resourcePath, e);
        }
    }
}
