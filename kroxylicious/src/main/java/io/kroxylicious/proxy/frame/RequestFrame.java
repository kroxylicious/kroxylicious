/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

public interface RequestFrame extends Frame {

    /**
     * Whether the response to this request should be decoded.
     * @return Whether the response to this request should be decoded.
     */
    boolean decodeResponse();

}
