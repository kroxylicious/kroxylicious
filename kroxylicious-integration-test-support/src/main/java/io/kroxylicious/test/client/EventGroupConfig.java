/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public record EventGroupConfig(
        Class<? extends SocketChannel> clientChannelClass,
        Class<? extends ServerSocketChannel> serverChannelClass
) {

    public static EventGroupConfig create() {
        final Class<? extends SocketChannel> clientChannelClass;
        final Class<? extends ServerSocketChannel> serverChannelClass;

        if (Epoll.isAvailable()) {
            clientChannelClass = EpollSocketChannel.class;
            serverChannelClass = EpollServerSocketChannel.class;
        } else if (KQueue.isAvailable()) {
            clientChannelClass = KQueueSocketChannel.class;
            serverChannelClass = KQueueServerSocketChannel.class;
        } else {
            clientChannelClass = NioSocketChannel.class;
            serverChannelClass = NioServerSocketChannel.class;
        }
        return new EventGroupConfig(clientChannelClass, serverChannelClass);
    }

    private static EventLoopGroup newGroup(int nThreads) {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(nThreads);
        } else if (KQueue.isAvailable()) {
            return new KQueueEventLoopGroup(nThreads);
        } else {
            return new NioEventLoopGroup(nThreads);
        }
    }

    public EventLoopGroup newWorkerGroup() {
        return newGroup(1);
    }

    public EventLoopGroup newBossGroup() {
        return newGroup(1);
    }
}
