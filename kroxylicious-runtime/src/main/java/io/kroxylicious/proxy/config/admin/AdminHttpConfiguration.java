/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.admin;

public record AdminHttpConfiguration(
        String host,
        Integer port,
        EndpointsConfiguration endpoints
) {
    public AdminHttpConfiguration(String host, Integer port, EndpointsConfiguration endpoints) {
        this.host = host == null ? "0.0.0.0" : host;
        this.port = port == null ? 9190 : port;
        this.endpoints = endpoints == null ? new EndpointsConfiguration(null) : endpoints;
    }
}
