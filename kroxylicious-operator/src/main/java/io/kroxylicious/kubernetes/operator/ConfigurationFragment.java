/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Set;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

/**
 * A piece of proxy configuration which references files on the proxy container filesystem
 * @param fragment The piece of proxy configuration
 * @param volumes The volumes the configuration depends on
 * @param mounts The mount the configuration depends on
 * @param <F> The type of fragment
 */
public record ConfigurationFragment<F>(F fragment, Set<Volume> volumes, Set<VolumeMount> mounts) {
    public <T> ConfigurationFragment<T> map(Function<F, T> mapper) {
        return new ConfigurationFragment<>(mapper.apply(fragment), volumes, mounts);
    }
}
