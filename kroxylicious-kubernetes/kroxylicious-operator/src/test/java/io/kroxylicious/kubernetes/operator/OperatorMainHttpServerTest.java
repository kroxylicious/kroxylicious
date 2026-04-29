/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.RestoreSystemProperties;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.junitpioneer.jupiter.SetSystemProperty;

import com.sun.net.httpserver.HttpServer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@RestoreSystemProperties
class OperatorMainHttpServerTest {

    // Note each test case needs to specify its own bind address because on certain platforms (*stares at* Mac Os)
    // we can't re-bind to the same address quickly enough for the tests to work

    private HttpServer httpServer;

    @BeforeEach
    void setUp() {
        httpServer = null;
    }

    @AfterEach
    void tearDown() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    @Test
    @SetEnvironmentVariable(key = "BIND_ADDRESS", value = "127.0.0.1:8888")
    void shouldBindToConfiguredAddress() {
        // Given

        // When
        var bindAddress = OperatorMain.getBindAddress();

        // Then
        assertThat(bindAddress.getAddress().getHostAddress()).isEqualTo("127.0.0.1");
        assertThat(bindAddress.getPort()).isEqualTo(8888);
    }

    @Test
    @SetEnvironmentVariable(key = "BIND_ADDRESS", value = "127.0.0.1")
    void shouldBindToDefaultPortForPartiallySpecifiedAddress() {
        // Given
        // When
        var bindAddress = OperatorMain.getBindAddress();

        // Then
        assertThat(bindAddress.getAddress().getHostAddress()).isEqualTo("127.0.0.1");
        assertThat(bindAddress.getPort()).isEqualTo(8080);
    }

    @Test
    @ClearEnvironmentVariable(key = "BIND_ADDRESS")
    void shouldBindToDefaultAddress() {
        assertDefaultBinding();
    }

    @Test
    @SetEnvironmentVariable(key = "BIND_ADDRESS", value = "")
    void shouldBindToDefaultAddressForEmptyVar() {
        assertDefaultBinding();
    }

    @Test
    @SetEnvironmentVariable(key = "BIND_ADDRESS", value = "127.0.0.1:14565")
    void shouldLimitRequestTime() throws IOException {
        // Given

        // When
        httpServer = OperatorMain.createHttpServer();

        // Then
        assertThat(System.getProperty("sun.net.httpserver.maxReqTime")).isEqualTo("60");
    }

    @Test
    @SetEnvironmentVariable(key = "BIND_ADDRESS", value = "127.0.0.1:14576")
    void shouldLimitResponseTime() throws IOException {
        // Given

        // When
        httpServer = OperatorMain.createHttpServer();

        // Then
        assertThat(System.getProperty("sun.net.httpserver.maxRspTime")).isEqualTo("120");
    }

    @Test
    @SetEnvironmentVariable(key = "BIND_ADDRESS", value = "127.0.0.1:12565")
    @SetSystemProperty(key = "sun.net.httpserver.maxReqTime", value = "12")
    void shouldRespectUserConfigurationForRequestTimeout() throws IOException {
        // Given

        // When
        httpServer = OperatorMain.createHttpServer();

        // Then
        assertThat(System.getProperty("sun.net.httpserver.maxReqTime")).isEqualTo("12");
    }

    @Test
    @SetEnvironmentVariable(key = "BIND_ADDRESS", value = "127.0.0.1:14574")
    @SetSystemProperty(key = "sun.net.httpserver.maxRspTime", value = "12")
    void shouldRespectUserConfigurationForResponseTimeout() throws IOException {
        // Given

        // When
        httpServer = OperatorMain.createHttpServer();

        // Then
        assertThat(System.getProperty("sun.net.httpserver.maxRspTime")).isEqualTo("12");
    }

    private void assertDefaultBinding() {
        // Given
        // When
        var bindAddress = OperatorMain.getBindAddress().getAddress();

        // Then
        assertThat(bindAddress.isAnyLocalAddress()).isTrue();
        if (bindAddress instanceof Inet4Address) {
            assertThat(bindAddress.getHostAddress()).isEqualTo("0.0.0.0");
        }
        else if (bindAddress instanceof Inet6Address) {
            assertThat(bindAddress.getHostAddress()).isEqualTo("0:0:0:0:0:0:0:0");
        }
        else {
            fail("Unknown  bindAddress type " + bindAddress.getClass());
        }
    }

}