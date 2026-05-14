/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.math.BigInteger;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Date;

import javax.security.auth.x500.X500Principal;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
