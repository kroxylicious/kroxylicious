/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testcluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.stream.Stream;

public class Utils {
    public static Stream<Integer> preAllocateListeningPorts(int num) {
        if (num < 1) {
            return Stream.of();
        }
        try (var serverSocket = new ServerSocket(0)) {
            serverSocket.setReuseAddress(true);
            int localPort = serverSocket.getLocalPort();
            return Stream.concat(List.of(localPort).stream(), preAllocateListeningPorts(num - 1));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
