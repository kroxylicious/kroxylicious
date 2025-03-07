/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.HasMetadata;

// Utility methods for working with fabric8 generated Custom Resource classes
public class Resources {

    private Resources() {
    }

    public static String name(HasMetadata resource) {
        return resource.getMetadata().getName();
    }

    public static String namespace(HasMetadata resource) {
        return resource.getMetadata().getNamespace();
    }

    public static Long generation(HasMetadata resource) {
        return resource.getMetadata().getGeneration();
    }

    public static String uid(HasMetadata resource) {
        return resource.getMetadata().getUid();
    }

    public static <T extends HasMetadata> Map<String, T> indexByName(Stream<T> stream) {
        return stream.collect(Collectors.toMap(Resources::name, Function.identity()));
    }
}
