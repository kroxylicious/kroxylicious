/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

/**
 * A piece of proxy configuration which references files on the proxy container filesystem.
 * @param fragment The piece of proxy configuration.
 * @param volumes The volumes the configuration depends on.
 * @param mounts The mount the configuration depends on.
 * @param <F> The type of fragment.
 */
public record ConfigurationFragment<F>(F fragment, Set<Volume> volumes, Set<VolumeMount> mounts) {

    /**
     * @return An empty optional fragment.
     * @param <F> The fragment type.
     */
    static <F> ConfigurationFragment<Optional<F>> empty() {
        return new ConfigurationFragment<>(Optional.empty(), Set.of(), Set.of());
    }

    /**
     * Combine two fragments into a new fragment by applying the given function and taking the union of their volumes and mounts.
     * @param x The first fragment to combine.
     * @param y The first fragment to combine.
     * @param fn The combining function.
     * @return The new fragment.
     * @param <X> The type of the first fragment.
     * @param <Y> The type of the second fragment.
     * @param <F> The type of the result fragment.
     */
    public static <X, Y, F> ConfigurationFragment<F> combine(ConfigurationFragment<X> x, ConfigurationFragment<Y> y, BiFunction<X, Y, F> fn) {
        var t = fn.apply(x.fragment(), y.fragment());
        return new ConfigurationFragment<>(t,
                Stream.concat(x.volumes.stream(), y.volumes.stream()).collect(Collectors.toSet()),
                Stream.concat(x.mounts.stream(), y.mounts.stream()).collect(Collectors.toSet()));
    }

    /**
     * Apply the given mapping function to the fragment, returning a new instance with the same volumes and mounts.
     * @param mapper The mapping function.
     * @return A new fragment.
     * @param <G> The type of the new fragment.
     */
    public <G> ConfigurationFragment<G> map(Function<F, G> mapper) {
        return new ConfigurationFragment<>(mapper.apply(fragment), volumes, mounts);
    }

    public <G> ConfigurationFragment<G> flatMap(Function<F, ConfigurationFragment<G>> mapper) {
        ConfigurationFragment<G> apply = mapper.apply(fragment);
        return new ConfigurationFragment<>(apply.fragment(),
                Stream.concat(volumes.stream(), apply.volumes.stream()).collect(Collectors.toSet()),
                Stream.concat(mounts.stream(), apply.mounts.stream()).collect(Collectors.toSet()));
    }

    public static <F> ConfigurationFragment<List<F>> reduce(List<ConfigurationFragment<F>> fragments) {
        var list = fragments.stream().map(ConfigurationFragment::fragment).toList();
        return new ConfigurationFragment<>(list,
                fragments.stream().flatMap(cfs -> cfs.volumes().stream()).collect(Collectors.toSet()),
                fragments.stream().flatMap(cfs -> cfs.mounts().stream()).collect(Collectors.toSet()));
    }

}
