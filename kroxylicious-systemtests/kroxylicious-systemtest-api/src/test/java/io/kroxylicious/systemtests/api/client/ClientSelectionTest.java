/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.systemtests.api.ClientLocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClientSelectionTest {

    @Test
    void defaultsWhenUnset() {
        assertThat(ClientSelection.fromEnvironment(Map.of()))
                .isEqualTo(new ClientSelection(ClientDriver.JAVA, ClientExecutionEnvironment.ON_CLUSTER));
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "  " })
    void defaultsWhenBlank(String blank) {
        var environment = Map.of(ClientSelection.DRIVER_ENV_VAR, blank, ClientSelection.LOCATION_ENV_VAR, blank);
        assertThat(ClientSelection.fromEnvironment(environment))
                .isEqualTo(new ClientSelection(ClientSelection.DEFAULT_DRIVER, ClientSelection.DEFAULT_EXECUTION_ENVIRONMENT));
    }

    @ParameterizedTest
    @CsvSource({
            "java, in-process, JAVA, IN_PROCESS",
            "librdkafka, on-cluster, LIBRDKAFKA, ON_CLUSTER",
            "SARAMA, ON-CLUSTER, SARAMA, ON_CLUSTER",
            "' kcat ', ' in-process ', KCAT, IN_PROCESS"
    })
    void readsBothAxes(String driverValue, String locationValue, ClientDriver expectedDriver, ClientExecutionEnvironment expectedEnvironment) {
        var environment = Map.of(ClientSelection.DRIVER_ENV_VAR, driverValue, ClientSelection.LOCATION_ENV_VAR, locationValue);
        assertThat(ClientSelection.fromEnvironment(environment))
                .isEqualTo(new ClientSelection(expectedDriver, expectedEnvironment));
    }

    @Test
    void rejectsUnknownDriver() {
        var environment = Map.of(ClientSelection.DRIVER_ENV_VAR, "franz-go");
        assertThatThrownBy(() -> ClientSelection.fromEnvironment(environment))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("franz-go")
                .hasMessageContaining("[java, librdkafka, sarama, kcat]");
    }

    @Test
    void rejectsUnknownLocation() {
        var environment = Map.of(ClientSelection.LOCATION_ENV_VAR, "off-cluster");
        assertThatThrownBy(() -> ClientSelection.fromEnvironment(environment))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("off-cluster")
                .hasMessageContaining("[in-process, on-cluster]");
    }

    @ParameterizedTest
    @EnumSource(ClientDriver.class)
    void driverEnvValueRoundTrips(ClientDriver driver) {
        assertThat(ClientDriver.fromEnvValue(driver.envValue())).isEqualTo(driver);
    }

    @ParameterizedTest
    @EnumSource(ClientExecutionEnvironment.class)
    void executionEnvironmentEnvValueRoundTrips(ClientExecutionEnvironment environment) {
        assertThat(ClientExecutionEnvironment.fromEnvValue(environment.envValue())).isEqualTo(environment);
    }

    @Test
    void executionEnvironmentMapsToBootstrapLocation() {
        assertThat(ClientExecutionEnvironment.IN_PROCESS.clientLocation()).isEqualTo(ClientLocation.OFF_CLUSTER);
        assertThat(ClientExecutionEnvironment.ON_CLUSTER.clientLocation()).isEqualTo(ClientLocation.ON_CLUSTER);
    }
}
