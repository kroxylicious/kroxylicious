/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.function.Supplier;

import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import io.netty.incubator.channel.uring.IOUring;

public enum NettyTransport {
    IO_URING(IOUring::isAvailable),
    EPOLL(Epoll::isAvailable),
    KQUEUE(KQueue::isAvailable),
    NIO(() -> true);

    // using a supplier to avoid invoking potentially expensive checks when not needed.
    private final Supplier<Boolean> isAvailable;

    NettyTransport(Supplier<Boolean> isAvailable) {
        this.isAvailable = isAvailable;
    }

    public boolean isAvailable() {
        return this.isAvailable.get();
    }
}
