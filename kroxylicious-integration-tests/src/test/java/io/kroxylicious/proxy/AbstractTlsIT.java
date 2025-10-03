/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLHandshakeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.proxy.config.secret.FilePassword;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.common.KafkaClusterConfig;
import io.kroxylicious.testing.kafka.common.KafkaClusterFactory;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTlsIT extends BaseIT {
    static final HostPort PROXY_ADDRESS = HostPort.parse("localhost:9192");
    static final String TOPIC = "my-test-topic";

    static final String SNI_BASE_ADDRESS = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName("sni");
    static final String SNI_BROKER_ADDRESS_PATTERN = "broker-$(nodeId)." + SNI_BASE_ADDRESS;
    static final HostPort SNI_BOOTSTRAP_ADDRESS = HostPort.parse("bootstrap." + SNI_BASE_ADDRESS + ":9192");

    @TempDir
    Path certsDirectory;
    KeytoolCertificateGenerator downstreamCertificateGenerator;
    KeytoolCertificateGenerator clientCertGenerator;
    Path clientTrustStore;
    Path proxyTrustStore;

    static PasswordProvider constructPasswordProvider(Class<? extends PasswordProvider> providerClazz, String password) {
        if (providerClazz.equals(InlinePassword.class)) {
            return new InlinePassword(password);
        }
        else if (providerClazz.equals(FilePassword.class)) {
            return new FilePassword(writePasswordToFile(password));
        }
        else {
            throw new IllegalArgumentException("Unexpected provider class: " + providerClazz);
        }
    }

    @NonNull
    static String writePasswordToFile(String password) {
        try {
            File tmp = File.createTempFile("password", ".txt");
            tmp.deleteOnExit();
            makeFileOwnerReadWriteOnly(tmp);
            Files.writeString(tmp.toPath(), password);
            // remove write from owner
            assertThat(tmp.setWritable(false, true)).isTrue();
            return tmp.getAbsolutePath();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write password to file", e);
        }
    }

    static File writeTrustToTemporaryFile(List<X509Certificate> certificates) {
        try {
            var file = File.createTempFile("trust", ".pem");
            makeFileOwnerReadWriteOnly(file);
            file.deleteOnExit();
            var mimeLineEnding = new byte[]{ '\r', '\n' };

            try (var out = new FileOutputStream(file)) {
                certificates.forEach(c -> {
                    var encoder = Base64.getMimeEncoder();
                    try {
                        out.write("-----BEGIN CERTIFICATE-----".getBytes(StandardCharsets.UTF_8));
                        out.write(mimeLineEnding);
                        out.write(encoder.encode(c.getEncoded()));
                        out.write(mimeLineEnding);
                        out.write("-----END CERTIFICATE-----".getBytes(StandardCharsets.UTF_8));
                        out.write(mimeLineEnding);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    catch (CertificateEncodingException e) {
                        throw new RuntimeException(e);
                    }
                });
                assertThat(file.setWritable(false, true)).isTrue();
                return file;
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write trust to temporary file", e);
        }
    }

    private static void makeFileOwnerReadWriteOnly(File f) {
        // remove read/write from everyone
        assertThat(f.setReadable(false, false)).isTrue();
        assertThat(f.setWritable(false, false)).isTrue();
        // add read/write for owner
        assertThat(f.setReadable(true, true)).isTrue();
        assertThat(f.setWritable(true, true)).isTrue();
    }

    @BeforeEach
    void beforeEach() throws Exception {
        // Note that the KeytoolCertificateGenerator generates key stores that are PKCS12 format.
        this.downstreamCertificateGenerator = new KeytoolCertificateGenerator();
        this.downstreamCertificateGenerator.generateSelfSignedCertificateEntry("test@kroxylicious.io", "localhost", "KI", "kroxylicious.io", null, null, "US");
        this.clientTrustStore = certsDirectory.resolve("kafka.truststore.jks");
        this.downstreamCertificateGenerator.generateTrustStore(this.downstreamCertificateGenerator.getCertFilePath(), "client",
                clientTrustStore.toAbsolutePath().toString());

        // Generator for certificate that will identify the client
        this.clientCertGenerator = new KeytoolCertificateGenerator();
        this.clientCertGenerator.generateSelfSignedCertificateEntry("clientTest@kroxylicious.io", "client", "Dev", "kroxylicious.io", null, null, "US");
        this.proxyTrustStore = certsDirectory.resolve("proxy.truststore.jks");
        this.clientCertGenerator.generateTrustStore(clientCertGenerator.getCertFilePath(), "proxy",
                proxyTrustStore.toAbsolutePath().toString());
    }

    KafkaCluster createMTlsCluster(KeytoolCertificateGenerator brokerCert, KeytoolCertificateGenerator clientCert) throws Exception {
        // Note that the annotation driven Kroxylicious Extension doesn't support configuring a cluster that expects client-auth.

        var cluster = KafkaClusterFactory.create(KafkaClusterConfig.builder()
                .brokerKeytoolCertificateGenerator(brokerCert)
                // note passing client generator causes ssl.client.auth to be set 'required'
                .clientKeytoolCertificateGenerator(clientCert)
                .kraftMode(true)
                .securityProtocol("SSL")
                .build());
        cluster.start();

        var config = new HashMap<>(cluster.getKafkaClientConfiguration());

        // Get the client key material provided by the framework. This will be used to configure Kroxylicious.
        var keyStore = config.get("ssl.keystore.location").toString();
        var keyPassword = config.get("ssl.keystore.password").toString();
        assertThat(keyStore).isNotNull();
        assertThat(keyPassword).isNotNull();

        var trustStore = config.get("ssl.truststore.location").toString();
        var trustPassword = config.get("ssl.truststore.password").toString();
        assertThat(trustStore).isNotNull();
        assertThat(trustPassword).isNotNull();

        // Validate a TLS client-auth connection to direct the cluster succeeds/fails as expected.
        assertSuccessfulDirectClientAuthConnectionWithClientCert(cluster);
        assertUnsuccessfulDirectClientAuthConnectionWithoutClientCert(cluster);

        return cluster;
    }

    void assertSuccessfulDirectClientAuthConnectionWithClientCert(KafkaCluster cluster) {
        try (var admin = CloseableAdmin.create(cluster.getKafkaClientConfiguration())) {
            // Any operation to test successful connection to cluster
            var result = admin.describeCluster().clusterId();
            assertThat(result).succeedsWithin(Duration.ofSeconds(10));
        }
    }

    void assertUnsuccessfulDirectClientAuthConnectionWithoutClientCert(KafkaCluster cluster) {
        var config = new HashMap<>(cluster.getKafkaClientConfiguration());

        // remove the client's certificate that the framework has provides.
        assertThat(config.remove("ssl.keystore.location")).isNotNull();
        assertThat(config.remove("ssl.keystore.password")).isNotNull();

        try (var admin = CloseableAdmin.create(config)) {
            // Any operation to test that connection to cluster fails as we don't present a certificate.
            assertThatThrownBy(() -> admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(SSLHandshakeException.class)
                    .rootCause()
                    .satisfiesAnyOf(e -> assertThat(e).hasMessageContaining("Received fatal alert: bad_certificate") /* <JDK-25 */,
                            e -> assertThat(e).hasMessageContaining("Received fatal alert: certificate_required") /* JDK-25 */);

        }
    }

}
