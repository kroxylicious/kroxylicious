/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

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

        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");

        // Load certificate chain
        Collection<? extends Certificate> certs;
        try (InputStream certStream = Files.newInputStream(certPath)) {
            certs = certFactory.generateCertificates(certStream);
        }

        // Load private key
        PrivateKey privateKey = loadPrivateKey(keyPath);

        // Build keystore
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setKeyEntry("webhook",
                privateKey,
                new char[0],
                certs.toArray(new Certificate[0]));

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, new char[0]);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), null, null);
        return sslContext;
    }

    @NonNull
    private static PrivateKey loadPrivateKey(@NonNull Path keyPath) throws IOException, GeneralSecurityException {
        String keyPem = Files.readString(keyPath);
        // Strip PEM headers/footers and whitespace
        String keyBase64 = keyPem
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replace("-----BEGIN RSA PRIVATE KEY-----", "")
                .replace("-----END RSA PRIVATE KEY-----", "")
                .replace("-----BEGIN EC PRIVATE KEY-----", "")
                .replace("-----END EC PRIVATE KEY-----", "")
                .replaceAll("\\s", "");

        byte[] keyBytes = Base64.getDecoder().decode(keyBase64);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);

        // Try RSA first, then EC
        for (String algorithm : List.of("RSA", "EC")) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(keySpec);
            }
            catch (GeneralSecurityException ignored) {
                // Try next algorithm
            }
        }
        throw new GeneralSecurityException("Unable to load private key from " + keyPath + " — unsupported key type");
    }
}
