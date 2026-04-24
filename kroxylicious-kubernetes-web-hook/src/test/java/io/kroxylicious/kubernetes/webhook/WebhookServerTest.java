/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Base64;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.RestoreSystemProperties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;

import io.kroxylicious.proxy.tls.CertificateGenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the HTTPS webhook server and its TLS configuration.
 */
@RestoreSystemProperties
class WebhookServerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Path certPath;
    private static Path keyPath;

    private WebhookServer server;

    @BeforeAll
    static void generateCertificates() throws IOException {
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        java.security.cert.X509Certificate cert = CertificateGenerator.generateSelfSignedX509Certificate(keyPair);
        certPath = CertificateGenerator.generateCertPem(cert);
        keyPath = writePkcs8Pem(keyPair);
        System.setProperty(
                "jdk.internal.httpclient.disableHostnameVerification", "true");
    }

    private static Path writePkcs8Pem(KeyPair keyPair) throws IOException {
        byte[] encoded = keyPair.getPrivate().getEncoded();
        String base64 = Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(encoded);
        String pem = "-----BEGIN PRIVATE KEY-----\n" + base64 + "\n-----END PRIVATE KEY-----\n";
        Path path = Files.createTempFile("pkcs8key", ".pem");
        path.toFile().deleteOnExit();
        Files.writeString(path, pem);
        return path;
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    // --- SSL context tests ---

    @Test
    void createSslContextWithRsaKey() throws Exception {
        SSLContext ctx = WebhookServer.createSslContext(certPath, keyPath);

        assertThat(ctx).isNotNull();
        SSLEngine engine = ctx.createSSLEngine();
        assertThat(engine).isNotNull();
    }

    @Test
    void createSslContextThrowsForMissingCertFile() {
        Path missing = Path.of("/nonexistent/cert.pem");

        assertThatThrownBy(() -> WebhookServer.createSslContext(missing, keyPath))
                .isInstanceOf(IOException.class);
    }

    @Test
    void createSslContextThrowsForMissingKeyFile() {
        Path missing = Path.of("/nonexistent/key.pem");

        assertThatThrownBy(() -> WebhookServer.createSslContext(certPath, missing))
                .isInstanceOf(IOException.class);
    }

    @Test
    void createSslContextThrowsForUnsupportedKeyType(@TempDir Path tempDir) throws Exception {
        Path badKey = tempDir.resolve("bad.pem");
        byte[] randomBytes = new byte[64];
        new SecureRandom().nextBytes(randomBytes);
        String badPem = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getEncoder().encodeToString(randomBytes)
                + "\n-----END PRIVATE KEY-----\n";
        Files.writeString(badKey, badPem);

        assertThatThrownBy(() -> WebhookServer.createSslContext(certPath, badKey))
                .isInstanceOf(GeneralSecurityException.class);
    }

    // --- Server lifecycle tests ---

    @Test
    void livezReturns200() throws Exception {
        int port = findFreePort();
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);
        server = new WebhookServer(addr, certPath, keyPath, defaultHandler());
        server.start();

        HttpResponse<String> response = httpsGet(port, "/livez");

        assertThat(response.statusCode()).isEqualTo(200);
    }

    @Test
    void mutateDelegatesToAdmissionHandler() throws Exception {
        int port = findFreePort();
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);

        server = new WebhookServer(addr, certPath, keyPath, defaultHandler());
        server.start();

        AdmissionReview review = new AdmissionReview();
        review.setApiVersion("admission.k8s.io/v1");
        review.setKind("AdmissionReview");
        AdmissionRequest request = new AdmissionRequest();
        request.setUid("test-uid");
        review.setRequest(request);

        byte[] requestBody = MAPPER.writeValueAsBytes(review);
        HttpResponse<String> response = httpsPost(port, "/mutate", requestBody);

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode responseJson = MAPPER.readTree(response.body());
        assertThat(responseJson.get("response").get("allowed").asBoolean()).isTrue();
        assertThat(responseJson.get("response").get("uid").asText()).isEqualTo("test-uid");
    }

    @Test
    void closeStopsServer() throws Exception {
        int port = findFreePort();
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);
        server = new WebhookServer(addr, certPath, keyPath, defaultHandler());
        server.start();

        server.close();
        server = null;

        assertThatThrownBy(() -> httpsGet(port, "/livez"))
                .hasCauseInstanceOf(ConnectException.class);
    }

    // --- helpers ---

    private static int findFreePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }

    private static AdmissionHandler defaultHandler() {
        return new AdmissionHandler(new SidecarConfigResolver(), "image:latest");
    }

    private static HttpResponse<String> httpsGet(
                                                 int port,
                                                 String path)
            throws Exception {
        HttpClient client = trustAllClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://127.0.0.1:" + port + path))
                .GET()
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> httpsPost(
                                                  int port,
                                                  String path,
                                                  byte[] body)
            throws Exception {
        HttpClient client = trustAllClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @SuppressWarnings("java:S4830")
    private static HttpClient trustAllClient() throws Exception {
        TrustManager[] trustAll = new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, trustAll, new SecureRandom());

        return HttpClient.newBuilder()
                .sslContext(ctx)
                .build();
    }
}
