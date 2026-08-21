/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

/**
 * Records the reason a client connection is being closed, for structured logging.
 *
 * @param category coarse-grained category used as a log key
 * @param detail   human-readable description of why the close was requested
 */
public record CloseReason(Category category, String detail) {

    public enum Category {
        ROUTER_REQUESTED,
        FILTER_CLOSE_CONNECTION,
        PROXY_SHUTDOWN
    }

    public static CloseReason routerRequested() {
        return new CloseReason(Category.ROUTER_REQUESTED, "router returned closeConnection=true");
    }

    public static CloseReason filterCloseConnection() {
        return new CloseReason(Category.FILTER_CLOSE_CONNECTION, "filter requested connection close");
    }

    public static CloseReason proxyShutdown() {
        return new CloseReason(Category.PROXY_SHUTDOWN, "proxy shutdown or virtual-cluster reload");
    }
}
