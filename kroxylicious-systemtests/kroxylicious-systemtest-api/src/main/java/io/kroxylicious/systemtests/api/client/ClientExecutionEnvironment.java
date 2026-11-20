/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

import java.util.Locale;
import java.util.Objects;

import io.kroxylicious.systemtests.api.ClientLocation;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Where a {@link KafkaClient} runs. Selected independently of the test
 * (see {@link ClientSelection#LOCATION_ENV_VAR}).
 */
public enum ClientExecutionEnvironment {

    /** The client runs inside the test JVM (directly, or via the Java Foreign Function Interface). */
    IN_PROCESS("in-process", ClientLocation.OFF_CLUSTER),

    /** The client runs as a workload (e.g. a Kubernetes Job) on the cluster hosting the proxy. */
    ON_CLUSTER("on-cluster", ClientLocation.ON_CLUSTER);

    private final String envValue;
    private final ClientLocation clientLocation;

    ClientExecutionEnvironment(String envValue, ClientLocation clientLocation) {
        this.envValue = envValue;
        this.clientLocation = clientLocation;
    }

    /**
     * The value used to select this environment via {@link ClientSelection#LOCATION_ENV_VAR}.
     *
     * @return environment variable value
     */
    @NonNull
    public String envValue() {
        return envValue;
    }

    /**
     * The location of a client running in this environment, relative to the proxy.
     * Use it to pick the matching bootstrap address from
     * {@link io.kroxylicious.systemtests.api.ProxyHandle#bootstrap(ClientLocation)}.
     *
     * @return client location
     */
    @NonNull
    public ClientLocation clientLocation() {
        return clientLocation;
    }

    /**
     * Parses an environment variable value (case-insensitive) into an execution environment.
     *
     * @param value the value, e.g. {@code on-cluster}
     * @return the execution environment
     * @throws IllegalArgumentException if the value does not name a known environment
     */
    @NonNull
    public static ClientExecutionEnvironment fromEnvValue(@NonNull String value) {
        Objects.requireNonNull(value, "value must not be null");
        String normalised = value.trim().toLowerCase(Locale.ROOT);
        for (ClientExecutionEnvironment environment : values()) {
            if (environment.envValue.equals(normalised)) {
                return environment;
            }
        }
        throw new IllegalArgumentException(
                "Unknown client location '" + value + "', expected one of " + EnvValues.of(values(), ClientExecutionEnvironment::envValue));
    }
}
