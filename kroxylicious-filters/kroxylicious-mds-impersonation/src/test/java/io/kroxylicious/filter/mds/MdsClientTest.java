/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.testing.certificate.CertificateGenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MdsClientTest {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(strings = { "alice", "new-client" })
    void authenticatesAsProxyAndRequestsTheClientsIdentity(String user) throws Exception {
        // Given
        try (var mds = new TestMdsServer(directory); var client = mds.client(mds.config(mds.clientTls))) {
            mds.response = TestMdsServer.tokenResponse(user);
            // When
            var token = client.impersonate(user).toCompletableFuture().get(10, TimeUnit.SECONDS);

            // Then
            assertThat(token.value()).isNotBlank();
            assertThat(mds.peerCertificate).isEqualTo(mds.proxyCertificate);
            assertThat(mds.requestMethod).isEqualTo("POST");
            assertThat(new ObjectMapper().readTree(mds.requestBody)).isEqualTo(
                    new ObjectMapper().readTree("{\"targetPrincipalType\":\"User\",\"targetPrincipalName\":\"" + user + "\"}"));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 301, 307, 401, 403, 500 })
    void rejectsHttpErrorsAndDoesNotFollowRedirects(int status) throws Exception {
        // Given
        try (var mds = new TestMdsServer(directory); var client = mds.client(mds.config(mds.clientTls))) {
            mds.status = status;
            mds.response = "remote error containing sensitive data";

            // When
            var result = client.impersonate("alice").toCompletableFuture();

            // Then
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("MDS authentication failed: MDS_HTTP (code=" + status + ")");
            assertThat(mds.requests).hasValue(1);
        }
    }

    @Test
    void mdsRejectsAnUntrustedProxyCertificate() throws Exception {
        // Given
        try (var mds = new TestMdsServer(directory)) {
            var otherKeys = CertificateGenerator.generateRsaKeyPair();
            var otherCert = CertificateGenerator.generateSelfSignedX509Certificate(otherKeys);
            var key = TestMdsServer.writeIdentity(directory, "untrusted", otherKeys, otherCert);
            try (var client = mds.client(mds.config(new Tls(key, mds.clientTls.trust(), null, null)))) {
                // When
                var result = client.impersonate("alice").toCompletableFuture();

                // Then
                assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS)).isInstanceOf(java.util.concurrent.ExecutionException.class);
                assertThat(mds.requests).hasValue(0);
            }
        }
    }

    @Test
    void proxyRejectsAnUntrustedMdsCertificate() throws Exception {
        // Given
        try (var mds = new TestMdsServer(directory);
                var client = mds.client(mds.config(new Tls(mds.clientTls.key(), null, null, null)))) {
            // When
            var result = client.impersonate("alice").toCompletableFuture();

            // Then
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS)).isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThat(mds.requests).hasValue(0);
        }
    }

    @Test
    void boundsHttpResponseSize() throws Exception {
        // Given
        try (var mds = new TestMdsServer(directory); var client = mds.client(mds.config(mds.clientTls))) {
            mds.response = "x".repeat(65537);

            // When
            var result = client.impersonate("alice").toCompletableFuture();

            // Then
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("MDS authentication failed: RESPONSE_SIZE (code=0)");
        }
    }

}
