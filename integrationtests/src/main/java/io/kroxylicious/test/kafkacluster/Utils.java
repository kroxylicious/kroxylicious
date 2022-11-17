/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.kafkacluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.stream.Stream;

public class Utils {
    /**
     * Pre-allocate 1 or more ephemeral ports which are available for use.
     *
     * @param num number of ports to pre-allocate
     * @return list of ephemeral ports
     */
    public static Stream<Integer> preAllocateListeningPorts(int num) {
        // Uses recursive algorithm to avoid the risk of returning a duplicate ephemeral port.
        if (num < 1) {
            return Stream.of();
        }
        try (var serverSocket = new ServerSocket(0)) {
            serverSocket.setReuseAddress(true);
            int localPort = serverSocket.getLocalPort();
            return Stream.concat(Stream.of(localPort), preAllocateListeningPorts(num - 1));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
