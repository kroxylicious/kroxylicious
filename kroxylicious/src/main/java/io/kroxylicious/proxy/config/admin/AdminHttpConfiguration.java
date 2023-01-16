/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.admin;

public class AdminHttpConfiguration {
    private final String host;
    private final Integer port;
    private final EndpointsConfiguration endpoints;

    public AdminHttpConfiguration(String host, Integer port, EndpointsConfiguration endpoints) {
        this.host = host == null ? "0.0.0.0" : host;
        this.port = port == null ? 9193 : port;
        this.endpoints = endpoints == null ? new EndpointsConfiguration(null) : endpoints;
    }

    public EndpointsConfiguration getEndpoints() {
        return endpoints;
    }

    public Integer getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }
}
