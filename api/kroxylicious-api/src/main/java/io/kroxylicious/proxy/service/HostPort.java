/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import java.util.Objects;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Represents a host port pair.
 *
 * @param host host name
 * @param port port number.
 */
public record HostPort(String host, int port) {
    private static final Pattern IPV6_WITH_PORT = Pattern.compile("^(\\[.+]):(.+)$");
    private static final Pattern PORT_SEPARATOR = Pattern.compile(":");

    /**
     * Creates a host port.
     */
     public HostPort {
        Objects.requireNonNull(host, "host cannot be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var hostPort = (HostPort) o;
        return port == hostPort.port && host.equals(hostPort.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    /**
     * Parses a host port pair from a string representation.
     *
     * For the host part, symbolic hostname, FQDN, IPv4, and IPv6 forms are supported.  In the case of IPv6,
     * the notation specified by <a href="https://www.rfc-editor.org/rfc/rfc4038#section-5.1">rfc4038</a> must be used.
     *
     * For the port part, it must be numeric.
     *
     * @param address stringified form of the host port, separated by colon (:).
     * @return a {@link HostPort}
     */
    @JsonCreator
    public static HostPort parse(String address) {
        var exceptionText = "unexpected address formation '%s'. Valid formations are 'host:9092', 'host.example.com:9092', or '[::ffff:c0a8:1]:9092', ".formatted(address);

        if (address == null) {
            throw new IllegalArgumentException(exceptionText);
        }

        var ipv6Match = IPV6_WITH_PORT.matcher(address);
        if (ipv6Match.matches()) {
            var host = ipv6Match.group(1);
            var port = parsePort(exceptionText, ipv6Match.group(2));
            return new HostPort(host, port);
        }
        else {
            var split = PORT_SEPARATOR.split(address);
            if (split.length != 2) {
                throw new IllegalArgumentException(exceptionText);
            }
            var host = split[0];
            if (host.isEmpty() || host.isBlank()) {
                throw new IllegalArgumentException(exceptionText);
            }
            var port = parsePort(exceptionText, split[1]);
            return new HostPort(host, port);
        }
    }

    private static int parsePort(String exceptionText, String group) {
        try {
            return Integer.parseInt(group);
        }
        catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(exceptionText, nfe);
        }
    }
}
