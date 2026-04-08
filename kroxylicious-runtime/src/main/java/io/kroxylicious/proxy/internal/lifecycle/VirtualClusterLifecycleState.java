/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.lifecycle;

/**
 * Represents the lifecycle state of a virtual cluster within the proxy.
 */
public enum VirtualClusterLifecycleState {

    /** Being set up. Not yet serving traffic. */
    INITIALIZING,

    /** Ports bound, accepting connections. */
    SERVING,

    /** Rejecting new connections. Waiting for in-flight connections to complete. */
    DRAINING,

    /** Configuration not viable. Resources released. Error captured. */
    FAILED,

    /** Permanently removed. Terminal state. */
    STOPPED
}
