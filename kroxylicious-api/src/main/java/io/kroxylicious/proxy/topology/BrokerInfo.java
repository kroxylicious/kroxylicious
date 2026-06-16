/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.topology;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Broker metadata: host, port, and optional rack assignment.
 *
 * @param host the broker hostname
 * @param port the broker port
 * @param rack the rack assignment, or null if not configured
 */
public record BrokerInfo(String host, int port, @Nullable String rack) {}
