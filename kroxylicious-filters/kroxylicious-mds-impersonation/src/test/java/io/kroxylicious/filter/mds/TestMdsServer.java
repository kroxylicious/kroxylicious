/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.testing.certificate.CertificateGenerator;
import io.kroxylicious.testing.kms.tls.TlsHttpClientConfigurator;

/** Local MDS protocol stand-in with independent server and proxy TLS identities. */
final class TestMdsServer implements AutoCloseable {
    final HttpsServer server;
    final Tls clientTls;
    final X509Certificate proxyCertificate;
    final AtomicInteger requests = new AtomicInteger();
    volatile int status = 200;
    volatile String requestBody;
    volatile String requestMethod;
    volatile X509Certificate peerCertificate;
    volatile String response = tokenResponse();

    TestMdsServer(Path directory) throws Exception {
        var proxyKeys = CertificateGenerator.generateRsaKeyPair();
        var serverKeys = CertificateGenerator.generateRsaKeyPair();
        proxyCertificate = CertificateGenerator.generateSelfSignedX509Certificate(proxyKeys);
        var serverCertificate = CertificateGenerator.generateSelfSignedX509Certificate(serverKeys);
        KeyPair proxyKey = writeIdentity(directory, "proxy", proxyKeys, proxyCertificate);
        KeyPair serverKey = writeIdentity(directory, "mds", serverKeys, serverCertificate);
        clientTls = new Tls(proxyKey, new TrustStore(serverKey.certificateFile(), null, "PEM"), null, null);
        var serverTls = new Tls(serverKey, new TrustStore(proxyKey.certificateFile(), null, "PEM"), null, null);
        SSLContext sslContext;
        try (var tlsClient = new TlsHttpClientConfigurator(serverTls).apply(HttpClient.newBuilder()).build()) {
            sslContext = tlsClient.sslContext();
        }
        server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
            @Override
            public void configure(HttpsParameters parameters) {
                var ssl = getSSLContext().getDefaultSSLParameters();
                ssl.setNeedClientAuth(true);
                parameters.setSSLParameters(ssl);
            }
        });
        server.createContext("/security/1.0/impersonate", exchange -> {
            try (exchange) {
                peerCertificate = (X509Certificate) ((HttpsExchange) exchange).getSSLSession().getPeerCertificates()[0];
                requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                requestMethod = exchange.getRequestMethod();
                requests.incrementAndGet();
                byte[] body = response.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.getResponseHeaders().add("Location", "/security/1.0/impersonate");
                exchange.sendResponseHeaders(status, body.length);
                exchange.getResponseBody().write(body);
            }
        });
        server.start();
    }

    static KeyPair writeIdentity(Path directory, String name, java.security.KeyPair keys, X509Certificate certificate) throws Exception {
        Path key = directory.resolve(name + ".key");
        Path cert = directory.resolve(name + ".crt");
        writePem(key, "PRIVATE KEY", keys.getPrivate().getEncoded());
        writePem(cert, "CERTIFICATE", certificate.getEncoded());
        return new KeyPair(key.toString(), cert.toString(), null);
    }

    private static void writePem(Path path, String type, byte[] bytes) throws IOException {
        Files.writeString(path, "-----BEGIN " + type + "-----\n" + Base64.getMimeEncoder(64, new byte[]{ '\n' }).encodeToString(bytes)
                + "\n-----END " + type + "-----\n");
    }

    static String tokenResponse() {
        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                ("{\"exp\":" + Instant.now().plusSeconds(600).getEpochSecond() + "}").getBytes(StandardCharsets.UTF_8));
        return "{\"auth_token\":\"eyJhbGciOiJSUzI1NiJ9." + payload + ".c2lnbmF0dXJl\",\"token_type\":\"Bearer\",\"expires_in\":600}";
    }

    MdsImpersonationConfig config(Tls tls) {
        return new MdsImpersonationConfig(URI.create("https://localhost:" + server.getAddress().getPort() + "/security/1.0/impersonate"),
                tls, Duration.ofSeconds(5), Duration.ofSeconds(5));
    }

    MdsClient client(MdsImpersonationConfig config) {
        var http = new TlsHttpClientConfigurator(config.mdsTls()).apply(HttpClient.newBuilder())
                .followRedirects(HttpClient.Redirect.NEVER).connectTimeout(config.requestTimeout()).build();
        return new MdsClient(http, config);
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
