/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPrivateKey;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLPeerUnverifiedException;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test utilities for generating and manipulating TLS key material and certificates, and for
 * inspecting the certificate presented by a Netty {@link SslContext}.
 */
public final class TlsTestUtils {

    private TlsTestUtils() {
        // Utility class
    }

    /**
     * Enum for cryptographic approach to use in the test. Can be extended in the future with
     * new algorithms or combinations e.g for post quantum cryptography.
     */
    public enum Algorithm {
        RSA("RSA", 2048, "SHA256withRSA"),
        EC("EC", 256, "SHA256withECDSA"),;

        Algorithm(String name, int keySize, String signatureAlgorithm) {
            this.name = name;
            this.keySize = keySize;
            this.signatureAlgorithm = signatureAlgorithm;
        }

        private final String name;
        private final int keySize;
        private final String signatureAlgorithm;

        static Algorithm fromPrivateKey(final PrivateKey privateKey) {
            return Algorithm.valueOf(privateKey.getAlgorithm());
        }
    }

    /**
     * Record holding a private key and certificate in PEM format.
     *
     * @param privateKeyPem PKCS#8 private key in PEM format
     * @param certificatePem X.509 certificate in PEM format
     */
    public record KeyAndCert(String privateKeyPem, String certificatePem) {}

    /**
     * Generates a self-signed key pair and certificate for testing using BouncyCastle.
     * The certificate is valid for 1 year from generation.
     *
     * @param algorithm algorithm name to when generating the key pair e.g. RSA
     * @return KeyAndCert record containing PEM-encoded private key and certificate
     * @throws Exception if key generation or certificate creation fails
     */
    public static KeyAndCert generateKeyAndCert(final Algorithm algorithm) throws Exception {
        final KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm.name);
        keyGen.initialize(algorithm.keySize, new SecureRandom());
        KeyPair keyPair = keyGen.generateKeyPair();

        final String certPem = generateCertForKey(keyPair);
        final String keyPem = toPem(keyPair.getPrivate());

        return new KeyAndCert(keyPem, certPem);
    }

    /**
     * Generates a new certificate for an existing key pair (simulates certificate renewal).
     *
     * @param keyPair the existing key pair to generate a certificate for
     * @return PEM-encoded certificate
     * @throws Exception if certificate generation fails
     */
    public static String generateCertForKey(final KeyPair keyPair) throws Exception {
        final Instant now = Instant.now();
        final Date notBefore = Date.from(now);
        final Date notAfter = Date.from(now.plusSeconds(365L * 24 * 60 * 60));

        final X500Name issuer = new X500Name("CN=Test Certificate,O=Kroxylicious,C=US");
        final BigInteger serial = new BigInteger(64, new SecureRandom());

        final X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                issuer,
                serial,
                notBefore,
                notAfter,
                issuer,
                keyPair.getPublic());

        final String signatureAlgorithm = Algorithm.valueOf(keyPair.getPrivate().getAlgorithm()).signatureAlgorithm;
        final ContentSigner signer = new JcaContentSignerBuilder(signatureAlgorithm)
                .setProvider(new BouncyCastleProvider())
                .build(keyPair.getPrivate());

        final var cert = new JcaX509CertificateConverter()
                .setProvider(new BouncyCastleProvider())
                .getCertificate(certBuilder.build(signer));

        return toPem(cert);
    }

    /**
     * Parses a PEM-encoded private key back into a KeyPair for certificate generation.
     *
     * @param keyAndCert the KeyAndCert containing the PEM-encoded private key
     * @return KeyPair reconstructed from the PEM
     * @throws Exception if key parsing fails
     */
    public static KeyPair parseKeyPair(final KeyAndCert keyAndCert) throws Exception {
        final PEMParser pemParser = new PEMParser(new StringReader(keyAndCert.privateKeyPem()));
        final Object pemObject = pemParser.readObject();
        final JcaPEMKeyConverter converter = new JcaPEMKeyConverter();

        if (pemObject instanceof PEMKeyPair pemKeyPair) {
            // PKCS#1 format
            return converter.getKeyPair(pemKeyPair);
        }
        if (pemObject instanceof PrivateKeyInfo privateKeyInfo) {
            // PKCS#8 format
            final PrivateKey privateKey = converter.getPrivateKey(privateKeyInfo);
            // Extract public key from the private key (this works for RSA, EC)
            switch(Algorithm.fromPrivateKey(privateKey)) {
                case RSA:
                    final java.security.interfaces.RSAPrivateCrtKey rsaPrivate = (java.security.interfaces.RSAPrivateCrtKey) privateKey;
                    final java.security.spec.RSAPublicKeySpec publicKeySpec = new java.security.spec.RSAPublicKeySpec(
                            rsaPrivate.getModulus(), rsaPrivate.getPublicExponent());
                    final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                    final java.security.PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
                    return new java.security.KeyPair(publicKey, privateKey);
                case EC:
                    final ECPrivateKey ecPrivate = (ECPrivateKey) privateKey;
                    // For EC keys, we need to extract the public key from the certificate since we can't derive it from the private key alone
                    // Parse the certificate to get the public key
                    final CertificateFactory cf = CertificateFactory.getInstance("X.509");
                    final X509Certificate cert = (X509Certificate) cf.generateCertificate(
                            new ByteArrayInputStream(keyAndCert.certificatePem().getBytes(StandardCharsets.UTF_8)));
                    return new java.security.KeyPair(cert.getPublicKey(), privateKey);
                default:
                    throw new IllegalArgumentException("Unsupported key algorithm: " + privateKey.getAlgorithm());
            }
        }
        throw new IllegalArgumentException("Unexpected PEM object type: " + pemObject.getClass().getName());
    }

    /**
     * Converts an object to PEM format using BouncyCastle.
     * For private keys, outputs in PKCS#8 format.
     *
     * @param object the object to convert (PrivateKey, Certificate, etc.)
     * @return PEM-formatted string
     * @throws IOException if PEM writing fails
     */
    private static String toPem(final Object object) throws IOException {
        final StringWriter writer = new StringWriter();
        try (JcaPEMWriter pemWriter = new JcaPEMWriter(writer)) {
            // For PrivateKey, use PKCS8Generator to ensure PKCS#8 format
            if (object instanceof PrivateKey privateKey) {
                final JcaPKCS8Generator generator = new JcaPKCS8Generator(privateKey, null);
                pemWriter.writeObject(generator.generate());
            }
            else {
                pemWriter.writeObject(object);
            }
        }
        return writer.toString();
    }

    /**
     * Creates a JKS file containing the given key and certificate.
     *
     * @param jksPath path where the JKS file should be created
     * @param keyAndCert the key and certificate to store
     * @param password the password for the keystore and key
     * @throws Exception if JKS creation fails
     */
    public static void createJksFile(final Path jksPath, final KeyAndCert keyAndCert, final String password) throws Exception {
        // Parse the PEM key and certificate
        final PEMParser pemParser = new PEMParser(new StringReader(keyAndCert.privateKeyPem()));
        final Object pemObject = pemParser.readObject();
        final JcaPEMKeyConverter converter = new JcaPEMKeyConverter();

        final PrivateKey privateKey;
        if (pemObject instanceof PEMKeyPair pemKeyPair) {
            privateKey = converter.getKeyPair(pemKeyPair).getPrivate();
        }
        else if (pemObject instanceof PrivateKeyInfo privateKeyInfo) {
            privateKey = converter.getPrivateKey(privateKeyInfo);
        }
        else {
            throw new IllegalArgumentException("Unexpected PEM object type: " + pemObject.getClass().getName());
        }

        final CertificateFactory cf = CertificateFactory.getInstance("X.509");
        final X509Certificate cert = (X509Certificate) cf.generateCertificate(
                new ByteArrayInputStream(keyAndCert.certificatePem().getBytes(StandardCharsets.UTF_8)));

        // Create JKS keystore
        final KeyStore ks = java.security.KeyStore.getInstance("JKS");
        ks.load(null, password.toCharArray());
        ks.setKeyEntry("server", privateKey, password.toCharArray(), new X509Certificate[]{ cert });

        // Write to file
        try (java.io.FileOutputStream fos = new FileOutputStream(jksPath.toFile())) {
            ks.store(fos, password.toCharArray());
        }
    }

    /**
     * Asserts that an SslContext contains a specific certificate.
     *
     * @param sslContext the SslContext to check
     * @param expectedCertPem expected certificate in PEM format
     * @throws Exception if extraction or comparison fails
     */
    public static void assertSslContextHasCert(final SslContext sslContext, final String expectedCertPem) throws Exception {
        final String actualFingerprint = extractCertFingerprint(sslContext);
        final String expectedFingerprint = getCertFingerprint(expectedCertPem);
        assertThat(actualFingerprint)
                .describedAs("SslContext should contain expected certificate")
                .isEqualTo(expectedFingerprint);
    }

    /**
     * Asserts that two certificate PEM strings represent different certificates.
     *
     * @param certPem1 first certificate PEM string
     * @param certPem2 second certificate PEM string
     * @throws Exception if fingerprint computation fails
     */
    public static void assertSslContextsHaveDifferentCerts(final String certPem1, final String certPem2) throws Exception {
        final String fingerprint1 = getCertFingerprint(certPem1);
        final String fingerprint2 = getCertFingerprint(certPem2);
        assertThat(fingerprint1)
                .describedAs("Certificates should be different")
                .isNotEqualTo(fingerprint2);
    }

    /**
     * Retrieve the fingerprint for a certificate
     * @param certPem cetificate to examine (in PEM format)
     * @return the fingerprint
     * @throws Exception rethrows security and parsing errors
     */
    private static String getCertFingerprint(final String certPem) throws Exception {
        final CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        final X509Certificate cert = (X509Certificate) certFactory.generateCertificate(
                new ByteArrayInputStream(certPem.getBytes(StandardCharsets.UTF_8)));
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return Base64.getEncoder().encodeToString(digest.digest(cert.getEncoded()));
    }

    /**
     * Extracts the certificate from a Netty SslContext by creating SSL engines and
     * performing enough of a handshake to retrieve the server certificate.
     *
     * @param serverSslContext the server SslContext to extract the certificate from
     * @return SHA-256 fingerprint of the certificate
     * @throws Exception if extraction fails
     */
    private static String extractCertFingerprint(final SslContext serverSslContext) throws Exception {
        // For Netty SslContext, we need to perform a handshake to get the certificate.
        // Create a test client that will connect and extract the server's certificate.
        final SslContext clientContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

        final SSLEngine server = serverSslContext.newEngine(ByteBufAllocator.DEFAULT);
        final SSLEngine client = clientContext.newEngine(ByteBufAllocator.DEFAULT, "localhost", 9092);

        try {
            server.setUseClientMode(false);
            client.setUseClientMode(true);

            server.beginHandshake();
            client.beginHandshake();

            // Network buffers for encrypted data - start in write mode
            final ByteBuffer clientToServer = ByteBuffer.allocate(65536);
            final ByteBuffer serverToClient = ByteBuffer.allocate(65536);
            final ByteBuffer emptyAppData = ByteBuffer.allocate(0);
            final ByteBuffer appDataBuffer = ByteBuffer.allocate(65536);

            // Run handshake until we can get the peer certificate
            for (int round = 0; round < 100; round++) {
                // Process client engine
                SSLEngineResult.HandshakeStatus clientStatus = client.getHandshakeStatus();

                switch (clientStatus) {
                    case NEED_WRAP ->
                            // Client produces outbound data
                            client.wrap(emptyAppData, clientToServer);
                    case NEED_UNWRAP -> {
                        // Client consumes inbound data from server
                        if (serverToClient.position() > 0) {
                            serverToClient.flip(); // Switch to read mode
                            appDataBuffer.clear();
                            client.unwrap(serverToClient, appDataBuffer);
                            serverToClient.compact(); // Back to write mode
                        }
                    }
                    case NEED_TASK -> runDelegatedTasks(client);
                    case NOT_HANDSHAKING, FINISHED -> {
                        // Client handshake complete
                    }
                }

                // Try to extract certificate after processing client
                try {
                    final Certificate[] certs = client.getSession().getPeerCertificates();
                    if (certs != null && certs.length > 0) {
                        X509Certificate cert = (X509Certificate) certs[0];
                        MessageDigest digest = MessageDigest.getInstance("SHA-256");
                        return Base64.getEncoder().encodeToString(digest.digest(cert.getEncoded()));
                    }
                }
                catch (SSLPeerUnverifiedException e) {
                    // Not available yet
                }

                // Process server engine
                SSLEngineResult.HandshakeStatus serverStatus = server.getHandshakeStatus();

                switch (serverStatus) {
                    case NEED_WRAP ->
                            // Server produces outbound data
                            server.wrap(emptyAppData, serverToClient);
                    case NEED_UNWRAP -> {
                        // Server consumes inbound data from client
                        if (clientToServer.position() > 0) {
                            clientToServer.flip(); // Switch to read mode
                            appDataBuffer.clear();
                            server.unwrap(clientToServer, appDataBuffer);
                            clientToServer.compact(); // Back to write mode
                        }
                    }
                    case NEED_TASK -> runDelegatedTasks(server);
                    case NOT_HANDSHAKING, FINISHED -> {
                        // Server handshake complete
                    }
                }

                // Check if both engines are done
                if ((clientStatus == NOT_HANDSHAKING || clientStatus == FINISHED) &&
                        (serverStatus == NOT_HANDSHAKING || serverStatus == FINISHED)) {
                    break;
                }
            }

            // Final attempt to get certificate
            Certificate[] certs = client.getSession().getPeerCertificates();
            X509Certificate cert = (X509Certificate) certs[0];
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return Base64.getEncoder().encodeToString(digest.digest(cert.getEncoded()));
        }
        finally {
            ReferenceCountUtil.release(server);
            ReferenceCountUtil.release(client);
            ReferenceCountUtil.release(clientContext);
        }
    }

    private static void runDelegatedTasks(final SSLEngine engine) {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            task.run();
        }
    }

}
