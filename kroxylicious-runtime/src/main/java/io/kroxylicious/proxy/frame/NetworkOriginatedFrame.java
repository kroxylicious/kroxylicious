/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.frame;

/**
 * Signifies that a frame originated from the network.
 */
public interface NetworkOriginatedFrame {

    /**
     * The original encoded size of the frame when it arrived from the network.
     *
     * @return original encoded size.
     */
    int originalEncodedSize();

}
