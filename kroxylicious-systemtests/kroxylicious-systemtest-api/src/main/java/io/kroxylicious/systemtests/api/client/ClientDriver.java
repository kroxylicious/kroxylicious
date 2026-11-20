/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

import java.util.Locale;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The Kafka protocol implementation a {@link KafkaClient} uses to talk to the proxy.
 * <p>
 * Kroxylicious is a protocol proxy, so proving that it behaves correctly with clients built on
 * different protocol implementations is a first-class testing concern. The driver is selected
 * independently of the test (see {@link ClientSelection#DRIVER_ENV_VAR}).
 */
public enum ClientDriver {

    /** The Apache Kafka Java client. */
    JAVA("java"),

    /** librdkafka, the C client library used by confluent-kafka-python, confluent-kafka-go, etc. */
    LIBRDKAFKA("librdkafka"),

    /** Sarama, the pure Go client library. */
    SARAMA("sarama"),

    /** The kcat command line tool (built on librdkafka). */
    KCAT("kcat");

    private final String envValue;

    ClientDriver(String envValue) {
        this.envValue = envValue;
    }

    /**
     * The value used to select this driver via {@link ClientSelection#DRIVER_ENV_VAR}.
     *
     * @return environment variable value
     */
    @NonNull
    public String envValue() {
        return envValue;
    }

    /**
     * Parses an environment variable value (case-insensitive) into a driver.
     *
     * @param value the value, e.g. {@code java}
     * @return the driver
     * @throws IllegalArgumentException if the value does not name a known driver
     */
    @NonNull
    public static ClientDriver fromEnvValue(@NonNull String value) {
        Objects.requireNonNull(value, "value must not be null");
        String normalised = value.trim().toLowerCase(Locale.ROOT);
        for (ClientDriver driver : values()) {
            if (driver.envValue.equals(normalised)) {
                return driver;
            }
        }
        throw new IllegalArgumentException("Unknown client driver '" + value + "', expected one of " + EnvValues.of(values(), ClientDriver::envValue));
    }
}
