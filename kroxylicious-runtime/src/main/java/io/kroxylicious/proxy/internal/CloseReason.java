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

    /**
     * Coarse-grained category of a connection close, used as a structured log key.
     */
    public enum Category {
        /** A router returned {@code closeConnection=true}. */
        ROUTER_REQUESTED,
        /** A filter requested that the connection be closed. */
        FILTER_CLOSE_CONNECTION,
        /** The proxy is shutting down or reloading the virtual cluster. */
        PROXY_SHUTDOWN
    }

    /**
     * Creates a close reason for a router-requested close.
     *
     * @return the close reason
     */
    public static CloseReason routerRequested() {
        return new CloseReason(Category.ROUTER_REQUESTED, "router returned closeConnection=true");
    }

    /**
     * Creates a close reason for a filter-requested close.
     *
     * @return the close reason
     */
    public static CloseReason filterCloseConnection() {
        return new CloseReason(Category.FILTER_CLOSE_CONNECTION, "filter requested connection close");
    }

    /**
     * Creates a close reason for a proxy shutdown or virtual-cluster reload.
     *
     * @return the close reason
     */
    public static CloseReason proxyShutdown() {
        return new CloseReason(Category.PROXY_SHUTDOWN, "proxy shutdown or virtual-cluster reload");
    }
}
