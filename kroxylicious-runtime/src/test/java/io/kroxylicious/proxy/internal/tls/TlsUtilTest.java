/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Date;
import java.util.List;

import javax.security.auth.x500.X500Principal;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.KeyStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TlsUtilTest {

    private static TestCertificateUtil.KeyAndCert keyAndCert;

    @BeforeAll
    static void setUp() throws Exception {
        keyAndCert = TestCertificateUtil.generateKeyStoreAndCert();
    }

    @Nested
    class ValidateKeyAndCertMatch {

        @Test
        void acceptsMatchingRsaKeyAndCert() {
            TlsUtil.validateKeyAndCertMatch(keyAndCert.privateKey(), keyAndCert.cert());
        }

        @Test
        void rejectsMismatchedRsaKeyAndCert() throws Exception {
            TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateKeyStoreAndCert("CN=other");
            PrivateKey privateKey = other.privateKey();
            X509Certificate cert = keyAndCert.cert();
            assertThatThrownBy(() -> TlsUtil.validateKeyAndCertMatch(privateKey, cert))
                    .isInstanceOf(BadTlsCredentialsException.class)
                    .hasMessageContaining("does not match");
        }

        @Test
        void acceptsMatchingEcKeyAndCert() throws Exception {
            TestCertificateUtil.KeyAndCert ecKeyAndCert = TestCertificateUtil.generateEcKeyStoreAndCert("CN=ec-test", "secp256r1");
            TlsUtil.validateKeyAndCertMatch(ecKeyAndCert.privateKey(), ecKeyAndCert.cert());
        }

        @Test
        void rejectsMismatchedEcKeysOnSameCurve() throws Exception {
            TestCertificateUtil.KeyAndCert ec1 = TestCertificateUtil.generateEcKeyStoreAndCert("CN=ec1", "secp256r1");
            TestCertificateUtil.KeyAndCert ec2 = TestCertificateUtil.generateEcKeyStoreAndCert("CN=ec2", "secp256r1");
            PrivateKey privateKey = ec1.privateKey();
            X509Certificate cert = ec2.cert();
            assertThatThrownBy(() -> TlsUtil.validateKeyAndCertMatch(privateKey, cert))
                    .isInstanceOf(BadTlsCredentialsException.class)
                    .hasMessageContaining("does not correspond");
        }

        @Test
        void acceptsMatchingDsaKeyAndCert() throws Exception {
            TestCertificateUtil.KeyAndCert dsaKeyAndCert = TestCertificateUtil.generateDsaKeyStoreAndCert("CN=dsa-test");
            TlsUtil.validateKeyAndCertMatch(dsaKeyAndCert.privateKey(), dsaKeyAndCert.cert());
        }

        @Test
        void rejectsMismatchedDsaKeys() throws Exception {
            TestCertificateUtil.KeyAndCert dsa1 = TestCertificateUtil.generateDsaKeyStoreAndCert("CN=dsa1");
            TestCertificateUtil.KeyAndCert dsa2 = TestCertificateUtil.generateDsaKeyStoreAndCert("CN=dsa2");
            PrivateKey privateKey = dsa1.privateKey();
            X509Certificate cert = dsa2.cert();
            assertThatThrownBy(() -> TlsUtil.validateKeyAndCertMatch(privateKey, cert))
                    .isInstanceOf(BadTlsCredentialsException.class)
                    .hasMessageContaining("does not correspond");
        }

        @Test
        void rejectsAlgorithmMismatch() {
            PrivateKey mockKey = mock(PrivateKey.class);
            when(mockKey.getAlgorithm()).thenReturn("DSA");
            X509Certificate cert = keyAndCert.cert();
            assertThatThrownBy(() -> TlsUtil.validateKeyAndCertMatch(mockKey, cert))
                    .isInstanceOf(BadTlsCredentialsException.class)
                    .hasMessageContaining("does not match certificate public key algorithm");
        }

        @Test
        void acceptsMatchingUnknownAlgorithmByAlgorithmName() {
            PrivateKey privateKey = mock(PrivateKey.class);
            PublicKey publicKey = mock(PublicKey.class);
            X509Certificate cert = mock(X509Certificate.class);
            when(privateKey.getAlgorithm()).thenReturn("EdDSA");
            when(publicKey.getAlgorithm()).thenReturn("EdDSA");
            when(cert.getPublicKey()).thenReturn(publicKey);

            TlsUtil.validateKeyAndCertMatch(privateKey, cert);
        }

        @Test
        void rejectsRsaPrivateKeyWithWrongType() {
            PrivateKey privateKey = mock(PrivateKey.class);
            PublicKey publicKey = mock(RSAPublicKey.class);
            X509Certificate cert = mock(X509Certificate.class);
            when(privateKey.getAlgorithm()).thenReturn("RSA");
            when(publicKey.getAlgorithm()).thenReturn("RSA");
            when(cert.getPublicKey()).thenReturn(publicKey);

            assertThatThrownBy(() -> TlsUtil.validateKeyAndCertMatch(privateKey, cert))
                    .isInstanceOf(BadTlsCredentialsException.class)
                    .hasMessageContaining("Expected RSAPrivateKey");
        }

        @Test
        void rejectsRsaPublicKeyWithWrongType() {
            PrivateKey privateKey = mock(RSAPrivateKey.class);
            PublicKey publicKey = mock(PublicKey.class);
            X509Certificate cert = mock(X509Certificate.class);
            when(privateKey.getAlgorithm()).thenReturn("RSA");
            when(publicKey.getAlgorithm()).thenReturn("RSA");
            when(cert.getPublicKey()).thenReturn(publicKey);

            assertThatThrownBy(() -> TlsUtil.validateKeyAndCertMatch(privateKey, cert))
                    .isInstanceOf(BadTlsCredentialsException.class)
                    .hasMessageContaining("Expected RSAPublicKey");
        }

        @Test
        void rejectsRsaKeyWithNullModulus() {
            RSAPrivateKey privateKey = mock(RSAPrivateKey.class);
            RSAPublicKey publicKey = mock(RSAPublicKey.class);
            X509Certificate cert = mock(X509Certificate.class);
            when(privateKey.getAlgorithm()).thenReturn("RSA");
            when(publicKey.getAlgorithm()).thenReturn("RSA");
            when(privateKey.getModulus()).thenReturn(null);
            when(publicKey.getModulus()).thenReturn(BigInteger.ONE);
            when(cert.getPublicKey()).thenReturn(publicKey);

            assertThatThrownBy(() -> TlsUtil.validateKeyAndCertMatch(privateKey, cert))
                    .isInstanceOf(BadTlsCredentialsException.class)
                    .hasMessageContaining("RSA key modulus is null");
        }
    }

    @Nested
    class ValidateCertificateChain {

        @Test
        void acceptsSingleSelfSignedCert() {
            TlsUtil.validateCertificateChain(keyAndCert.privateKey(), new X509Certificate[]{ keyAndCert.cert() });
        }

        @Test
        void rejectsEmptyChain() {
            PrivateKey privateKey = keyAndCert.privateKey();
            assertThatThrownBy(() -> TlsUtil.validateCertificateChain(privateKey, new X509Certificate[0]))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("empty");
        }

        @Test
        void rejectsKeyMismatch() throws Exception {
            TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateKeyStoreAndCert("CN=other");
            PrivateKey privateKey = other.privateKey();
            X509Certificate[] certChain = { keyAndCert.cert() };
            assertThatThrownBy(() -> TlsUtil.validateCertificateChain(privateKey, certChain))
                    .isInstanceOf(BadTlsCredentialsException.class)
                    .hasMessageContaining("does not match");
        }

        @Test
        void acceptsCertWithClientAuthEku() throws Exception {
            TestCertificateUtil.KeyAndCert clientAuthCert = TestCertificateUtil.generateKeyStoreAndCert("CN=clientauth", "eku=clientAuth");
            TlsUtil.validateCertificateChain(clientAuthCert.privateKey(), new X509Certificate[]{ clientAuthCert.cert() });
        }

        @Test
        void acceptsCertWithNoEku() {
            // Default keytool certs have no EKU, which is unrestricted
            TlsUtil.validateCertificateChain(keyAndCert.privateKey(), new X509Certificate[]{ keyAndCert.cert() });
        }

        @Test
        void rejectsCertWithServerAuthOnlyEku() throws Exception {
            TestCertificateUtil.KeyAndCert serverOnlyCert = TestCertificateUtil.generateKeyStoreAndCert("CN=serveronly", "eku=serverAuth");
            PrivateKey privateKey = serverOnlyCert.privateKey();
            X509Certificate[] certChain = { serverOnlyCert.cert() };
            assertThatThrownBy(() -> TlsUtil.validateCertificateChain(privateKey, certChain))
                    .isInstanceOf(BadTlsCredentialsException.class)
                    .hasMessageContaining("clientAuth");
        }

        @Test
        void rejectsExpiredLeafCertificate() throws Exception {
            PrivateKey privateKey = mockPrivateKey("TEST");
            X509Certificate cert = mock(X509Certificate.class);
            doThrow(new CertificateExpiredException("expired")).when(cert).checkValidity(any(Date.class));
            when(cert.getNotBefore()).thenReturn(new Date(0));
            when(cert.getNotAfter()).thenReturn(new Date(1));

            assertThatThrownBy(() -> TlsUtil.validateCertificateChain(privateKey, new X509Certificate[]{ cert }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Leaf certificate is not valid")
                    .hasCauseInstanceOf(CertificateExpiredException.class);
        }

        @Test
        void rejectsUnreadableExtendedKeyUsage() throws Exception {
            PrivateKey privateKey = mockPrivateKey("TEST");
            X509Certificate cert = mockCertificate("CN=leaf", "CN=issuer", "TEST");
            doThrow(new CertificateParsingException("bad eku")).when(cert).getExtendedKeyUsage();

            assertThatThrownBy(() -> TlsUtil.validateCertificateChain(privateKey, new X509Certificate[]{ cert }))
                    .isInstanceOf(BadTlsCredentialsException.class)
                    .hasMessageContaining("Failed to read extended key usage")
                    .hasCauseInstanceOf(CertificateParsingException.class);
        }

        @Test
        void rejectsInvalidCertificateChainOrder() throws Exception {
            PrivateKey privateKey = mockPrivateKey("TEST");
            X509Certificate leaf = mockCertificate("CN=leaf", "CN=issuer", "TEST");
            X509Certificate wrongIssuer = mockCertificate("CN=wrong", "CN=root", "TEST");

            assertThatThrownBy(() -> TlsUtil.validateCertificateChain(privateKey, new X509Certificate[]{ leaf, wrongIssuer }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Certificate chain order is invalid");
        }

        @Test
        void rejectsCertificateSignatureFailure() throws Exception {
            PrivateKey privateKey = mockPrivateKey("TEST");
            X509Certificate leaf = mockCertificate("CN=leaf", "CN=issuer", "TEST");
            X509Certificate issuer = mockCertificate("CN=issuer", "CN=root", "TEST");
            PublicKey issuerPublicKey = issuer.getPublicKey();
            doThrow(new SignatureException("bad signature")).when(leaf).verify(issuerPublicKey);

            assertThatThrownBy(() -> TlsUtil.validateCertificateChain(privateKey, new X509Certificate[]{ leaf, issuer }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("signature verification failed")
                    .hasCauseInstanceOf(SignatureException.class);
        }

        @Test
        void rejectsInvalidIntermediateCertificateDates() throws Exception {
            PrivateKey privateKey = mockPrivateKey("TEST");
            X509Certificate leaf = mockCertificate("CN=leaf", "CN=issuer", "TEST");
            X509Certificate issuer = mockCertificate("CN=issuer", "CN=root", "TEST");
            doThrow(new CertificateExpiredException("expired")).when(issuer).checkValidity(any(Date.class));
            when(issuer.getNotBefore()).thenReturn(new Date(0));
            when(issuer.getNotAfter()).thenReturn(new Date(1));

            assertThatThrownBy(() -> TlsUtil.validateCertificateChain(privateKey, new X509Certificate[]{ leaf, issuer }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Intermediate certificate at position 0 is not valid")
                    .hasCauseInstanceOf(CertificateExpiredException.class);
        }
    }

    @Nested
    class ValidateCertificateKeyPair {

        @Test
        void returnsTrueForMatchingPkcs8KeyPair(@TempDir final Path dir) throws Exception {
            final KeyPair keyPair = writeKeyPair(dir, keyAndCert.privateKey(), keyAndCert.cert(), false);
            assertThat(TlsUtil.validateCertificateKeyPair(keyPair)).contains(true);
        }

        @Test
        void returnsTrueForMatchingPkcs1RsaKeyPair(@TempDir final Path dir) throws Exception {
            final KeyPair keyPair = writeKeyPair(dir, keyAndCert.privateKey(), keyAndCert.cert(), true);
            assertThat(TlsUtil.validateCertificateKeyPair(keyPair)).contains(true);
        }

        @Test
        void returnsTrueForMatchingEcKeyPair(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert ec = TestCertificateUtil.generateEcKeyStoreAndCert("CN=ec-test", "secp256r1");
            final KeyPair keyPair = writeKeyPair(dir, ec.privateKey(), ec.cert(), false);
            assertThat(TlsUtil.validateCertificateKeyPair(keyPair)).contains(true);
        }

        @Test
        void returnsTrueForMatchingSec1EcKeyPair(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert ec = TestCertificateUtil.generateEcKeyStoreAndCert("CN=ec-sec1", "secp256r1");
            final Path keyFile = Files.writeString(dir.resolve("key.pem"), TestCertificateUtil.toSec1EcPem(ec.privateKey()));
            final Path certFile = Files.writeString(dir.resolve("cert.pem"), TestCertificateUtil.toPem(ec.cert()));
            final KeyPair keyPair = new KeyPair(keyFile.toString(), certFile.toString(), null);
            assertThat(TlsUtil.validateCertificateKeyPair(keyPair)).contains(true);
        }

        @Test
        void returnsTrueForMatchingSec1EcKeyPairOnP384(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert ec = TestCertificateUtil.generateEcKeyStoreAndCert("CN=ec-sec1-384", "secp384r1");
            final Path keyFile = Files.writeString(dir.resolve("key.pem"), TestCertificateUtil.toSec1EcPem(ec.privateKey()));
            final Path certFile = Files.writeString(dir.resolve("cert.pem"), TestCertificateUtil.toPem(ec.cert()));
            final KeyPair keyPair = new KeyPair(keyFile.toString(), certFile.toString(), null);
            assertThat(TlsUtil.validateCertificateKeyPair(keyPair)).contains(true);
        }

        @Test
        void returnsFalseForMismatchedSec1EcKeyPair(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert ec = TestCertificateUtil.generateEcKeyStoreAndCert("CN=ec-sec1", "secp256r1");
            final TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateEcKeyStoreAndCert("CN=ec-other", "secp256r1");
            final Path keyFile = Files.writeString(dir.resolve("key.pem"), TestCertificateUtil.toSec1EcPem(ec.privateKey()));
            final Path certFile = Files.writeString(dir.resolve("cert.pem"), TestCertificateUtil.toPem(other.cert()));
            final KeyPair keyPair = new KeyPair(keyFile.toString(), certFile.toString(), null);
            assertThat(TlsUtil.validateCertificateKeyPair(keyPair)).contains(false);
        }

        @Test
        void returnsFalseForMismatchedKeyPair(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateKeyStoreAndCert("CN=other");
            final KeyPair keyPair = writeKeyPair(dir, other.privateKey(), keyAndCert.cert(), false);
            assertThat(TlsUtil.validateCertificateKeyPair(keyPair)).contains(false);
        }

        @Test
        void returnsEmptyForMissingKeyFile(@TempDir final Path dir) throws Exception {
            final Path certFile = Files.writeString(dir.resolve("cert.pem"), TestCertificateUtil.toPem(keyAndCert.cert()));
            final KeyPair keyPair = new KeyPair(dir.resolve("does-not-exist.pem").toString(), certFile.toString(), null);
            assertThat(TlsUtil.validateCertificateKeyPair(keyPair)).isEmpty();
        }

        @Test
        void returnsEmptyForMalformedPem(@TempDir final Path dir) throws Exception {
            final Path keyFile = Files.writeString(dir.resolve("key.pem"), "not a valid pem");
            final Path certFile = Files.writeString(dir.resolve("cert.pem"), "not a valid pem");
            final KeyPair keyPair = new KeyPair(keyFile.toString(), certFile.toString(), null);
            assertThat(TlsUtil.validateCertificateKeyPair(keyPair)).isEmpty();
        }

        @Test
        void returnsEmptyForEncryptedKeyPair(@TempDir final Path dir) throws Exception {
            final Path keyFile = Files.writeString(dir.resolve("key.pem"), TestCertificateUtil.toPkcs8Pem(keyAndCert.privateKey()));
            final Path certFile = Files.writeString(dir.resolve("cert.pem"), TestCertificateUtil.toPem(keyAndCert.cert()));
            final KeyPair keyPair = new KeyPair(keyFile.toString(), certFile.toString(), new InlinePassword("changeit"));
            assertThat(TlsUtil.validateCertificateKeyPair(keyPair)).isEmpty();
        }

        @Test
        void returnsTrueForMatchingPemKeyStore(@TempDir final Path dir) throws Exception {
            final KeyStore keyStore = writePemStore(dir, keyAndCert.privateKey(), keyAndCert.cert());
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).contains(true);
        }

        @Test
        void returnsFalseForMismatchedPemKeyStore(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateKeyStoreAndCert("CN=other");
            final KeyStore keyStore = writePemStore(dir, other.privateKey(), keyAndCert.cert());
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).contains(false);
        }

        @Test
        void returnsTrueForMatchingJksKeyStore(@TempDir final Path dir) throws Exception {
            final KeyStore keyStore = writeKeyStore(dir, keyAndCert.privateKey(), keyAndCert.cert(), "JKS");
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).contains(true);
        }

        @Test
        void returnsTrueForMatchingPkcs12KeyStore(@TempDir final Path dir) throws Exception {
            final KeyStore keyStore = writeKeyStore(dir, keyAndCert.privateKey(), keyAndCert.cert(), "PKCS12");
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).contains(true);
        }

        @Test
        void usesStorePasswordWhenKeyPasswordAbsent(@TempDir final Path dir) throws Exception {
            final Path file = writeKeyStoreFile(dir, keyAndCert.privateKey(), keyAndCert.cert(), "PKCS12");
            final KeyStore keyStore = new KeyStore(file.toString(), new InlinePassword(STORE_PASSWORD), null, "PKCS12");
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).contains(true);
        }

        @Test
        void returnsEmptyForWrongKeyStorePassword(@TempDir final Path dir) throws Exception {
            final Path file = writeKeyStoreFile(dir, keyAndCert.privateKey(), keyAndCert.cert(), "PKCS12");
            final KeyStore keyStore = new KeyStore(file.toString(), new InlinePassword("wrong-password"), null, "PKCS12");
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).isEmpty();
        }

        @Test
        void returnsEmptyForMissingKeyStoreFile(@TempDir final Path dir) {
            final KeyStore keyStore = new KeyStore(dir.resolve("does-not-exist.p12").toString(), new InlinePassword(STORE_PASSWORD), null, "PKCS12");
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).isEmpty();
        }

        @Test
        void returnsTrueWhenAllKeyEntriesMatch(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert second = TestCertificateUtil.generateKeyStoreAndCert("CN=second");
            final Path file = writeMultiEntryKeyStore(dir, "JKS",
                    new KeyStoreEntry("a", keyAndCert.privateKey(), STORE_PASSWORD, List.of(keyAndCert.cert())),
                    new KeyStoreEntry("b", second.privateKey(), STORE_PASSWORD, List.of(second.cert())));
            final KeyStore keyStore = new KeyStore(file.toString(), new InlinePassword(STORE_PASSWORD), new InlinePassword(STORE_PASSWORD), "JKS");
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).contains(true);
        }

        @Test
        void returnsFalseWhenAnyKeyEntryMismatches(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateKeyStoreAndCert("CN=other");
            final Path file = writeMultiEntryKeyStore(dir, "JKS",
                    new KeyStoreEntry("good", keyAndCert.privateKey(), STORE_PASSWORD, List.of(keyAndCert.cert())),
                    new KeyStoreEntry("bad", other.privateKey(), STORE_PASSWORD, List.of(keyAndCert.cert())));
            final KeyStore keyStore = new KeyStore(file.toString(), new InlinePassword(STORE_PASSWORD), new InlinePassword(STORE_PASSWORD), "JKS");
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).contains(false);
        }

        @Test
        void skipsKeyEntryThatCannotBeRecovered(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateKeyStoreAndCert("CN=other");
            final Path file = writeMultiEntryKeyStore(dir, "JKS",
                    new KeyStoreEntry("good", keyAndCert.privateKey(), STORE_PASSWORD, List.of(keyAndCert.cert())),
                    new KeyStoreEntry("locked", other.privateKey(), "different-password", List.of(keyAndCert.cert())));
            final KeyStore keyStore = new KeyStore(file.toString(), new InlinePassword(STORE_PASSWORD), new InlinePassword(STORE_PASSWORD), "JKS");
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).contains(true);
        }

        @Test
        void usesLeafCertificateFromEntryChain(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert extra = TestCertificateUtil.generateKeyStoreAndCert("CN=extra");
            // A certificate chain is ordered leaf-first, so element 0 is the leaf whose public key must
            // correspond to the private key. Here the matching cert is the leaf and `extra` is a later
            // (ignored) chain entry, so validation passes.
            final Path file = writeMultiEntryKeyStore(dir, "JKS",
                    new KeyStoreEntry("a", keyAndCert.privateKey(), STORE_PASSWORD, List.of(keyAndCert.cert(), extra.cert())));
            final KeyStore keyStore = new KeyStore(file.toString(), new InlinePassword(STORE_PASSWORD), new InlinePassword(STORE_PASSWORD), "JKS");
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).contains(true);
        }

        @Test
        void matchesAgainstLeafNotLaterChainCert(@TempDir final Path dir) throws Exception {
            final TestCertificateUtil.KeyAndCert extra = TestCertificateUtil.generateKeyStoreAndCert("CN=extra");
            // Only the leaf (chain element 0) is matched against the key. Here `extra` is deliberately placed
            // first so the leaf does NOT match the key, whilst the matching cert sits later in the chain where
            // it is ignored. This proves the ordering matters: validation fails because the leaf is checked.
            final Path file = writeMultiEntryKeyStore(dir, "JKS",
                    new KeyStoreEntry("a", keyAndCert.privateKey(), STORE_PASSWORD, List.of(extra.cert(), keyAndCert.cert())));
            final KeyStore keyStore = new KeyStore(file.toString(), new InlinePassword(STORE_PASSWORD), new InlinePassword(STORE_PASSWORD), "JKS");
            assertThat(TlsUtil.validateCertificateKeyPair(keyStore)).contains(false);
        }
    }

    private static final String STORE_PASSWORD = "changeit";

    private static KeyPair writeKeyPair(final Path dir, final PrivateKey privateKey, final X509Certificate cert, final boolean pkcs1) throws Exception {
        final String keyPem = pkcs1 ? TestCertificateUtil.toPkcs1Pem(privateKey) : TestCertificateUtil.toPkcs8Pem(privateKey);
        final Path keyFile = Files.writeString(dir.resolve("key.pem"), keyPem);
        final Path certFile = Files.writeString(dir.resolve("cert.pem"), TestCertificateUtil.toPem(cert));
        return new KeyPair(keyFile.toString(), certFile.toString(), null);
    }

    private static KeyStore writePemStore(final Path dir, final PrivateKey privateKey, final X509Certificate cert) throws Exception {
        final String pem = TestCertificateUtil.toPkcs8Pem(privateKey) + TestCertificateUtil.toPem(cert);
        final Path file = Files.writeString(dir.resolve("store.pem"), pem);
        return new KeyStore(file.toString(), null, null, "PEM");
    }

    private static KeyStore writeKeyStore(final Path dir, final PrivateKey privateKey, final X509Certificate cert, final String storeType) throws Exception {
        final Path file = writeKeyStoreFile(dir, privateKey, cert, storeType);
        return new KeyStore(file.toString(), new InlinePassword(STORE_PASSWORD), new InlinePassword(STORE_PASSWORD), storeType);
    }

    private static Path writeKeyStoreFile(final Path dir, final PrivateKey privateKey, final X509Certificate cert, final String storeType) throws Exception {
        final java.security.KeyStore ks = java.security.KeyStore.getInstance(storeType);
        ks.load(null, null);
        ks.setKeyEntry("test", privateKey, STORE_PASSWORD.toCharArray(), new X509Certificate[]{ cert });
        final Path file = dir.resolve("store." + storeType.toLowerCase(java.util.Locale.ROOT));
        try (var os = Files.newOutputStream(file)) {
            ks.store(os, STORE_PASSWORD.toCharArray());
        }
        return file;
    }

    private static Path writeMultiEntryKeyStore(final Path dir, final String storeType, final KeyStoreEntry... entries) throws Exception {
        final java.security.KeyStore ks = java.security.KeyStore.getInstance(storeType);
        ks.load(null, null);
        for (final KeyStoreEntry entry : entries) {
            ks.setKeyEntry(entry.alias(), entry.key(), entry.password().toCharArray(), entry.chain().toArray(X509Certificate[]::new));
        }
        final Path file = dir.resolve("multi." + storeType.toLowerCase(java.util.Locale.ROOT));
        try (var os = Files.newOutputStream(file)) {
            ks.store(os, STORE_PASSWORD.toCharArray());
        }
        return file;
    }

    // `chain` is ordered leaf-first (element 0 is the leaf certificate, matching java.security.KeyStore
    // semantics where getCertificate(alias) returns the leaf and getCertificateChain(alias)[0] is the leaf).
    private record KeyStoreEntry(String alias, PrivateKey key, String password, List<X509Certificate> chain) {}

    private static PrivateKey mockPrivateKey(String algorithm) {
        PrivateKey privateKey = mock(PrivateKey.class);
        when(privateKey.getAlgorithm()).thenReturn(algorithm);
        return privateKey;
    }

    private static X509Certificate mockCertificate(String subjectName, String issuerName, String publicKeyAlgorithm) throws Exception {
        X509Certificate cert = mock(X509Certificate.class);
        PublicKey publicKey = mock(PublicKey.class);
        when(publicKey.getAlgorithm()).thenReturn(publicKeyAlgorithm);
        when(cert.getPublicKey()).thenReturn(publicKey);
        when(cert.getExtendedKeyUsage()).thenReturn(null);
        when(cert.getBasicConstraints()).thenReturn(-1);
        when(cert.getSubjectX500Principal()).thenReturn(new X500Principal(subjectName));
        when(cert.getIssuerX500Principal()).thenReturn(new X500Principal(issuerName));
        when(cert.getNotBefore()).thenReturn(new Date(0));
        when(cert.getNotAfter()).thenReturn(new Date(Long.MAX_VALUE));
        return cert;
    }

}
