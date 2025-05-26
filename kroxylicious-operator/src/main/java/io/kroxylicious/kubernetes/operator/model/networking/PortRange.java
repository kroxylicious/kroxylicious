/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

public record PortRange(int firstPort, int lastPort) {

    private static final int MIN_PORT = 0;
    private static final int MAX_PORT = 65535;

    public PortRange {
        validatePort("fistPort", firstPort);
        validatePort("lastPort", lastPort);
        if (lastPort < firstPort) {
            throw new IllegalArgumentException("Last port must be greater than or equal to first port.");
        }
    }

    private static void validatePort(String name, int port) {
        boolean isPort = port >= MIN_PORT && port <= MAX_PORT;
        if (!isPort) {
            throw new IllegalArgumentException("%s %d must be in the range (%d, %d)".formatted(name, port, MIN_PORT, MAX_PORT));
        }
    }

    public int portCount() {
        return lastPort - firstPort + 1;
    }

    public boolean overlaps(PortRange portRange) {
        return portRange.firstPort <= this.lastPort && this.firstPort <= portRange.lastPort;
    }

    public boolean contains(PortRange portRange) {
        return this.firstPort <= portRange.firstPort && this.lastPort >= portRange.lastPort;
    }
}
