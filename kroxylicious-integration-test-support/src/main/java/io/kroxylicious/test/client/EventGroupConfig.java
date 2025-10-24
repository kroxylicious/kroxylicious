/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueIoHandler;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringServerSocketChannel;
import io.netty.channel.uring.IoUringSocketChannel;

public record EventGroupConfig(
                               Class<? extends SocketChannel> clientChannelClass,
                               Class<? extends ServerSocketChannel> serverChannelClass) {

    public static EventGroupConfig create() {
        final Class<? extends SocketChannel> clientChannelClass;
        final Class<? extends ServerSocketChannel> serverChannelClass;

        if (IoUring.isAvailable()) {
            clientChannelClass = IoUringSocketChannel.class;
            serverChannelClass = IoUringServerSocketChannel.class;
        }
        else if (Epoll.isAvailable()) {
            clientChannelClass = EpollSocketChannel.class;
            serverChannelClass = EpollServerSocketChannel.class;
        }
        else if (KQueue.isAvailable()) {
            clientChannelClass = KQueueSocketChannel.class;
            serverChannelClass = KQueueServerSocketChannel.class;
        }
        else {
            clientChannelClass = NioSocketChannel.class;
            serverChannelClass = NioServerSocketChannel.class;
        }
        return new EventGroupConfig(clientChannelClass, serverChannelClass);
    }

    private static EventLoopGroup newGroup() {
        final IoHandlerFactory ioHandlerFactory;
        if (IoUring.isAvailable()) {
            ioHandlerFactory = IoUringIoHandler.newFactory();
        }
        else if (Epoll.isAvailable()) {
            ioHandlerFactory = EpollIoHandler.newFactory();
        }
        else if (KQueue.isAvailable()) {
            ioHandlerFactory = KQueueIoHandler.newFactory();
        }
        else {
            ioHandlerFactory = NioIoHandler.newFactory();
        }
        return new MultiThreadIoEventLoopGroup(1, ioHandlerFactory);
    }

    public EventLoopGroup newWorkerGroup() {
        return newGroup();
    }

    public EventLoopGroup newBossGroup() {
        return newGroup();
    }
}
