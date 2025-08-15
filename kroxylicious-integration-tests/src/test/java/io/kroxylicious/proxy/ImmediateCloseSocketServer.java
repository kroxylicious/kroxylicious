/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImmediateCloseSocketServer implements AutoCloseable {

    private final ServerSocket serverSocket;
    private final ExecutorService serverExecutor;
    private static final Logger LOGGER = LoggerFactory.getLogger(ImmediateCloseSocketServer.class);

    public ImmediateCloseSocketServer() {
        try {
            this.serverSocket = new ServerSocket(0, 50, InetAddress.getLocalHost());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.serverExecutor = Executors.newSingleThreadExecutor();
        this.serverExecutor.submit(this::serverLoop);
    }

    private void serverLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try (Socket clientSocket = this.serverSocket.accept()) {
                LOGGER.info("Accepted connection from {}, shutting down", clientSocket.getInetAddress());
            }
            catch (SocketException e) {
                break;
            }
            catch (Exception e) {
                LOGGER.error("Unhandled exception, continuing to accept connections", e);
            }
        }
    }

    public String getHostPort() {
        return this.serverSocket.getInetAddress().getHostName() + ":" + this.serverSocket.getLocalPort();
    }

    @Override
    public void close() {
        this.serverExecutor.shutdownNow();
        if (!this.serverSocket.isClosed()) {
            try {
                this.serverSocket.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

}
