/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import io.netty.channel.epoll.Epoll;
import io.netty.incubator.channel.uring.IOUring;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class NettyTest {
    @Test
    @EnabledOnOs(value = OS.LINUX)
    public void epollIsAvailableOnLinux() {
        assertTrue(Epoll.isAvailable());
    }

    @Test
    @EnabledOnOs(value = OS.LINUX)
    public void ioUringIsAvailableOnLinux() {
        assertTrue(IOUring.isAvailable());
    }
}
