/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.GeneralSecurityException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An HTTPS server hosting the admission webhook endpoint.
 */
class WebhookServer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebhookServer.class);

    private final HttpsServer server;

    WebhookServer(
                  @NonNull InetSocketAddress bindAddress,
                  @NonNull Path certPath,
                  @NonNull Path keyPath,
                  @NonNull AdmissionHandler admissionHandler)
            throws IOException, GeneralSecurityException {

        SSLContext sslContext = createSslContext(certPath, keyPath);
        server = HttpsServer.create(bindAddress, 0);
        server.setHttpsConfigurator(new HttpsConfigurator(sslContext));

        server.createContext("/mutate", admissionHandler);
        server.createContext("/livez", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(200, -1);
            }
        });

        LOGGER.atInfo()
                .addKeyValue("interface", bindAddress.getHostString())
                .addKeyValue("port", bindAddress.getPort())
                .log("Webhook server configured");
    }

    void start() {
        server.start();
        LOGGER.atInfo().log("Webhook server started");
    }

    @Override
    public void close() {
        server.stop(5);
        LOGGER.atInfo().log("Webhook server stopped");
    }

    @NonNull
    static SSLContext createSslContext(
                                       @NonNull Path certPath,
                                       @NonNull Path keyPath)
            throws GeneralSecurityException, IOException {
        SslContext nettyCtx;
        try {
            nettyCtx = SslContextBuilder
                    .forServer(certPath.toFile(), keyPath.toFile())
                    .sslProvider(SslProvider.JDK)
                    .build();
        }
        catch (SSLException | IllegalArgumentException e) {
            throw new GeneralSecurityException(
                    "Failed to load TLS credentials from cert=" + certPath + " key=" + keyPath, e);
        }
        if (nettyCtx instanceof JdkSslContext jdkCtx) {
            return jdkCtx.context();
        }
        throw new GeneralSecurityException(
                "Expected JdkSslContext but got " + nettyCtx.getClass().getName());
    }
}
