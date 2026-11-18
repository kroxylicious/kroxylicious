/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

import java.util.Map;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The combination of {@link ClientDriver} and {@link ClientExecutionEnvironment} that the framework
 * should use to build the {@link KafkaClient} handed to a test.
 * <p>
 * Tests never construct a selection themselves; the framework derives it from the environment
 * via {@link #fromEnvironment(Map)} so that CI can vary the two axes independently of the test.
 *
 * @param driver the protocol implementation
 * @param executionEnvironment where the client runs
 */
public record ClientSelection(@NonNull ClientDriver driver, @NonNull ClientExecutionEnvironment executionEnvironment) {

    /** Environment variable selecting the {@link ClientDriver}, e.g. {@code java}, {@code librdkafka}, {@code sarama}, {@code kcat}. */
    public static final String DRIVER_ENV_VAR = "KROXYLICIOUS_TEST_CLIENT_DRIVER";

    /** Environment variable selecting the {@link ClientExecutionEnvironment}, e.g. {@code in-process}, {@code on-cluster}. */
    public static final String LOCATION_ENV_VAR = "KROXYLICIOUS_TEST_CLIENT_LOCATION";

    /** The driver used when {@link #DRIVER_ENV_VAR} is unset or blank. */
    public static final ClientDriver DEFAULT_DRIVER = ClientDriver.JAVA;

    /** The execution environment used when {@link #LOCATION_ENV_VAR} is unset or blank. */
    public static final ClientExecutionEnvironment DEFAULT_EXECUTION_ENVIRONMENT = ClientExecutionEnvironment.ON_CLUSTER;

    public ClientSelection {
        Objects.requireNonNull(driver, "driver must not be null");
        Objects.requireNonNull(executionEnvironment, "executionEnvironment must not be null");
    }

    /**
     * Derives the selection from the process environment.
     *
     * @return the selection
     * @throws IllegalArgumentException if either variable holds an unknown value
     */
    @NonNull
    public static ClientSelection fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    /**
     * Derives the selection from the given environment, falling back to {@link #DEFAULT_DRIVER} and
     * {@link #DEFAULT_EXECUTION_ENVIRONMENT} for variables that are unset or blank.
     *
     * @param environment environment variables
     * @return the selection
     * @throws IllegalArgumentException if either variable holds an unknown value
     */
    @NonNull
    public static ClientSelection fromEnvironment(@NonNull Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment must not be null");
        String driver = environment.get(DRIVER_ENV_VAR);
        String location = environment.get(LOCATION_ENV_VAR);
        return new ClientSelection(
                isBlank(driver) ? DEFAULT_DRIVER : ClientDriver.fromEnvValue(driver),
                isBlank(location) ? DEFAULT_EXECUTION_ENVIRONMENT : ClientExecutionEnvironment.fromEnvValue(location));
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
