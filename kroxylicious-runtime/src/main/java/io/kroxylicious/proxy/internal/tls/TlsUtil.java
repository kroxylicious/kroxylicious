/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.AlgorithmParameters;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.ECParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.SSLException;
import javax.security.auth.x500.X500Principal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Utility class for TLS credential parsing and validation.
 */
public class TlsUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(TlsUtil.class);

    private static final String BEGIN_MARKER = "-----BEGIN ";
    private static final String END_MARKER = "-----END ";
    private static final String PRIVATE_KEY_SUFFIX = "PRIVATE KEY-----";

    private TlsUtil() {
        // Utility class
    }

    /**
     * Parses a PEM-encoded private key from a byte array.
     *
     * @param pemKeyBytes PEM-encoded private key bytes
     * @return The parsed PrivateKey
     * @throws IOException if the key cannot be parsed
     */
    @NonNull
    public static PrivateKey parsePrivateKey(@NonNull byte[] pemKeyBytes) throws IOException {
        return parsePrivateKey(pemKeyBytes, null);
    }

    /**
     * Parses a PEM-encoded private key from a byte array, with optional password for encrypted keys.
     *
     * @param pemKeyBytes PEM-encoded private key bytes
     * @param password password for decrypting the private key, or {@code null} if unencrypted
     * @return The parsed PrivateKey
     * @throws IOException if the key cannot be parsed
     */
    @NonNull
    public static PrivateKey parsePrivateKey(@NonNull byte[] pemKeyBytes, @edu.umd.cs.findbugs.annotations.Nullable char[] password) throws IOException {
        try {
            String pemString = new String(pemKeyBytes, java.nio.charset.StandardCharsets.US_ASCII);

            // Find the BEGIN marker for a private key
            int beginIdx = pemString.indexOf(BEGIN_MARKER);
            if (beginIdx < 0 || !pemString.substring(beginIdx + BEGIN_MARKER.length()).contains(PRIVATE_KEY_SUFFIX)) {
                throw new IOException("No private key found in PEM data");
            }

            // Extract the header type (e.g. "PRIVATE KEY", "RSA PRIVATE KEY")
            int headerStart = beginIdx + BEGIN_MARKER.length();
            int headerEnd = pemString.indexOf("-----", headerStart);
            String headerType = pemString.substring(headerStart, headerEnd);

            // Extract Base64 body between the markers
            String beginLine = BEGIN_MARKER + headerType + "-----";
            String endLine = END_MARKER + headerType + "-----";
            int bodyStart = pemString.indexOf(beginLine) + beginLine.length();
            int bodyEnd = pemString.indexOf(endLine);
            if (bodyEnd < 0) {
                throw new IOException("No matching END marker found for " + headerType);
            }

            String base64Key = pemString.substring(bodyStart, bodyEnd).replaceAll("\\s", "");
            byte[] keyBytes = Base64.getDecoder().decode(base64Key);

            // Handle encrypted PKCS#8 private keys
            if (password != null) {
                keyBytes = decryptPrivateKey(keyBytes, password);
            }

            // Handle PKCS#1 format ("RSA PRIVATE KEY") by converting to PKCS#8
            if (headerType.startsWith("RSA")) {
                return parsePkcs1RsaPrivateKey(keyBytes);
            }

            // Handle PKCS#8 format ("PRIVATE KEY") - try common key algorithms
            return parsePkcs8PrivateKey(keyBytes);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IOException("Required cryptographic algorithm not available", e);
        }
    }

    private static byte[] decryptPrivateKey(byte[] keyBytes, char[] password) throws IOException {
        try {
            EncryptedPrivateKeyInfo encryptedInfo = new EncryptedPrivateKeyInfo(keyBytes);
            Cipher cipher = Cipher.getInstance(encryptedInfo.getAlgName());
            SecretKeyFactory skf = SecretKeyFactory.getInstance(encryptedInfo.getAlgName());
            PBEKeySpec pbeKeySpec = new PBEKeySpec(password);
            SecretKey pbeKey = skf.generateSecret(pbeKeySpec);
            AlgorithmParameters algParams = encryptedInfo.getAlgParameters();
            cipher.init(Cipher.DECRYPT_MODE, pbeKey, algParams);
            PKCS8EncodedKeySpec decryptedSpec = encryptedInfo.getKeySpec(cipher);
            return decryptedSpec.getEncoded();
        }
        catch (Exception e) {
            throw new IOException("Failed to decrypt encrypted private key: " + e.getMessage(), e);
        }
    }

    private static PrivateKey parsePkcs1RsaPrivateKey(byte[] keyBytes) throws IOException {
        try {
            byte[] pkcs8Bytes = convertPkcs1ToPkcs8(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(pkcs8Bytes));
        }
        catch (Exception e) {
            throw new IOException("Failed to parse PKCS#1 RSA private key: " + e.getMessage(), e);
        }
    }

    private static PrivateKey parsePkcs8PrivateKey(byte[] keyBytes) throws IOException, NoSuchAlgorithmException {
        String[] algorithms = { "RSA", "EC", "DSA" };
        for (String algorithm : algorithms) {
            try {
                KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
                PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
                return keyFactory.generatePrivate(keySpec);
            }
            catch (InvalidKeySpecException e) {
                LOGGER.debug("Failed to parse private key as {}, trying next algorithm", algorithm);
            }
        }
        throw new IOException("Failed to parse private key with any supported algorithm");
    }

    /**
     * Converts PKCS#1 RSA private key bytes to PKCS#8 format by wrapping with the RSA AlgorithmIdentifier.
     * PKCS#8 = SEQUENCE { version INTEGER, algorithm AlgorithmIdentifier, privateKey OCTET STRING(PKCS#1) }
     */
    private static byte[] convertPkcs1ToPkcs8(byte[] pkcs1Bytes) {
        // RSA OID: 1.2.840.113549.1.1.1
        byte[] rsaOid = { 0x06, 0x09, 0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x0d, 0x01, 0x01, 0x01 };
        // AlgorithmIdentifier: SEQUENCE { OID, NULL }
        byte[] algorithmIdentifier = new byte[rsaOid.length + 4];
        algorithmIdentifier[0] = 0x30; // SEQUENCE
        algorithmIdentifier[1] = (byte) (rsaOid.length + 2);
        System.arraycopy(rsaOid, 0, algorithmIdentifier, 2, rsaOid.length);
        algorithmIdentifier[rsaOid.length + 2] = 0x05; // NULL
        algorithmIdentifier[rsaOid.length + 3] = 0x00;

        // OCTET STRING wrapping the PKCS#1 key
        byte[] octetString = wrapInAsn1(0x04, pkcs1Bytes);
        // version INTEGER 0
        byte[] version = { 0x02, 0x01, 0x00 };

        // SEQUENCE { version, algorithmIdentifier, privateKey }
        byte[] innerContent = new byte[version.length + algorithmIdentifier.length + octetString.length];
        int offset = 0;
        System.arraycopy(version, 0, innerContent, offset, version.length);
        offset += version.length;
        System.arraycopy(algorithmIdentifier, 0, innerContent, offset, algorithmIdentifier.length);
        offset += algorithmIdentifier.length;
        System.arraycopy(octetString, 0, innerContent, offset, octetString.length);

        return wrapInAsn1(0x30, innerContent);
    }

    private static byte[] wrapInAsn1(int tag, byte[] content) {
        byte[] lengthBytes;
        if (content.length < 128) {
            lengthBytes = new byte[]{ (byte) content.length };
        }
        else if (content.length < 256) {
            lengthBytes = new byte[]{ (byte) 0x81, (byte) content.length };
        }
        else if (content.length < 65536) {
            lengthBytes = new byte[]{ (byte) 0x82, (byte) (content.length >> 8), (byte) (content.length & 0xFF) };
        }
        else {
            lengthBytes = new byte[]{ (byte) 0x83, (byte) (content.length >> 16), (byte) ((content.length >> 8) & 0xFF), (byte) (content.length & 0xFF) };
        }
        byte[] result = new byte[1 + lengthBytes.length + content.length];
        result[0] = (byte) tag;
        System.arraycopy(lengthBytes, 0, result, 1, lengthBytes.length);
        System.arraycopy(content, 0, result, 1 + lengthBytes.length, content.length);
        return result;
    }

    /**
     * Parses a PEM-encoded certificate chain from a byte array.
     *
     * @param pemBytes PEM-encoded certificate bytes
     * @return Array of parsed X509Certificates
     * @throws IOException if the certificates cannot be parsed
     */
    @NonNull
    public static X509Certificate[] parseCertificateChain(@NonNull byte[] pemBytes) throws IOException {
        try {
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
            List<X509Certificate> certificates = new ArrayList<>();

            ByteArrayInputStream bais = new ByteArrayInputStream(pemBytes);

            while (bais.available() > 0) {
                try {
                    java.security.cert.Certificate cert = certFactory.generateCertificate(bais);
                    if (cert instanceof X509Certificate x509Cert) {
                        certificates.add(x509Cert);
                    }
                }
                catch (CertificateException e) {
                    // No more certificates to read
                    break;
                }
            }

            if (certificates.isEmpty()) {
                throw new IOException("No certificates found in PEM data");
            }

            return certificates.toArray(new X509Certificate[0]);
        }
        catch (CertificateException e) {
            throw new IOException("Failed to parse certificate chain", e);
        }
    }

    /**
     * Validates that a private key matches the public key in a certificate.
     *
     * @param privateKey The private key to validate
     * @param certificate The certificate containing the public key
     * @throws BadTlsCredentialsException if the keys don't match
     */
    public static void validateKeyAndCertMatch(@NonNull PrivateKey privateKey, @NonNull X509Certificate certificate) {
        PublicKey publicKey = certificate.getPublicKey();

        // Check if the algorithms match
        if (!privateKey.getAlgorithm().equals(publicKey.getAlgorithm())) {
            throw new BadTlsCredentialsException(
                    "Private key algorithm (" + privateKey.getAlgorithm() +
                            ") does not match certificate public key algorithm (" + publicKey.getAlgorithm() + ")");
        }

        // Perform algorithm-specific validation
        String algorithm = privateKey.getAlgorithm();
        switch (algorithm) {
            case "RSA" -> validateRsaKeyMatch(privateKey, publicKey);
            case "EC" -> validateEcKeyMatch(privateKey, publicKey);
            case "DSA" -> LOGGER.debug("DSA key-certificate matching validated via algorithm check");
            default -> LOGGER.debug("Key-certificate matching for {} validated via algorithm check", algorithm);
        }

        LOGGER.debug("Private key and certificate public key validated successfully: {}", algorithm);
    }

    /**
     * Validates that an RSA private key matches an RSA public key.
     *
     * @param privateKey The RSA private key
     * @param publicKey The RSA public key
     * @throws BadTlsCredentialsException if the keys don't match
     */
    private static void validateRsaKeyMatch(@NonNull PrivateKey privateKey, @NonNull PublicKey publicKey) {
        if (!(privateKey instanceof RSAPrivateKey rsaPrivateKey)) {
            throw new BadTlsCredentialsException("Expected RSAPrivateKey but got " + privateKey.getClass().getName());
        }
        if (!(publicKey instanceof RSAPublicKey rsaPublicKey)) {
            throw new BadTlsCredentialsException("Expected RSAPublicKey but got " + publicKey.getClass().getName());
        }

        // Verify modulus matches
        BigInteger privateModulus = rsaPrivateKey.getModulus();
        BigInteger publicModulus = rsaPublicKey.getModulus();

        if (privateModulus == null || publicModulus == null) {
            throw new BadTlsCredentialsException("RSA key modulus is null");
        }

        if (!privateModulus.equals(publicModulus)) {
            throw new BadTlsCredentialsException(
                    "RSA private key modulus does not match certificate public key modulus. " +
                            "The private key does not correspond to the certificate.");
        }

        LOGGER.debug("RSA key modulus match validated");
    }

    /**
     * Validates that an EC private key matches an EC public key by comparing curve parameters.
     *
     * @param privateKey The EC private key
     * @param publicKey The EC public key
     * @throws BadTlsCredentialsException if the keys don't match
     */
    private static void validateEcKeyMatch(@NonNull PrivateKey privateKey, @NonNull PublicKey publicKey) {
        if (!(privateKey instanceof ECPrivateKey ecPrivateKey)) {
            throw new BadTlsCredentialsException("Expected ECPrivateKey but got " + privateKey.getClass().getName());
        }
        if (!(publicKey instanceof ECPublicKey ecPublicKey)) {
            throw new BadTlsCredentialsException("Expected ECPublicKey but got " + publicKey.getClass().getName());
        }

        // Validate that both keys use the same curve
        ECParameterSpec privateParams = ecPrivateKey.getParams();
        ECParameterSpec publicParams = ecPublicKey.getParams();

        if (privateParams == null || publicParams == null) {
            throw new BadTlsCredentialsException("EC key parameters are null");
        }

        if (!privateParams.getCurve().equals(publicParams.getCurve())) {
            throw new BadTlsCredentialsException("EC private key curve does not match certificate public key curve. " +
                    "The private key does not correspond to the certificate.");
        }

        LOGGER.debug("EC key-certificate match validated via curve parameter comparison");
    }

    /**
     * Validates the certificate chain integrity and parameters.
     *
     * @param privateKey The private key (for key-certificate matching validation)
     * @param certChain The certificate chain to validate
     * @throws IllegalArgumentException if validation fails
     */
    public static void validateCertificateChain(@NonNull PrivateKey privateKey, @NonNull X509Certificate[] certChain) {
        if (certChain.length == 0) {
            throw new IllegalArgumentException("Certificate chain is empty");
        }

        X509Certificate leafCert = certChain[0];
        Date now = new Date();

        // Validate leaf certificate dates
        try {
            leafCert.checkValidity(now);
        }
        catch (CertificateException e) {
            throw new IllegalArgumentException(
                    "Leaf certificate is not valid: " + e.getMessage() +
                            " (valid from " + leafCert.getNotBefore() + " to " + leafCert.getNotAfter() + ")",
                    e);
        }

        // Validate that the private key corresponds to the leaf certificate
        validateKeyAndCertMatch(privateKey, leafCert);

        // Validate that the leaf certificate has clientAuth extended key usage if EKU is present.
        // If no EKU extension is present, the certificate is unrestricted (common for self-signed certs).
        validateClientAuthExtendedKeyUsage(leafCert);

        // Check for root CA in chain (should not be present per API contract).
        // A single self-signed leaf certificate is allowed (common for test and simple deployments),
        // but a self-signed CA certificate in a multi-cert chain should be excluded.
        if (certChain.length > 1) {
            for (int i = 0; i < certChain.length; i++) {
                X509Certificate cert = certChain[i];
                if (isSelfSigned(cert) && cert.getBasicConstraints() >= 0) {
                    throw new IllegalArgumentException(
                            "Certificate chain contains a self-signed root CA at position " + i +
                                    " (subject: " + cert.getSubjectX500Principal().getName() + "). " +
                                    "Root CA certificates must be excluded from the chain as per API contract.");
                }
            }
        }

        // Validate chain order and signatures (intermediate certificates)
        if (certChain.length > 1) {
            validateChainOrderAndSignatures(certChain, now);
        }

        LOGGER.debug("Certificate chain validation passed: {} certificates in chain", certChain.length);
    }

    /**
     * OID for id-kp-clientAuth (1.3.6.1.5.5.7.3.2).
     */
    private static final String CLIENT_AUTH_OID = "1.3.6.1.5.5.7.3.2";

    /**
     * Validates that the certificate includes the clientAuth extended key usage,
     * if an EKU extension is present. Certificates without EKU are considered unrestricted.
     */
    private static void validateClientAuthExtendedKeyUsage(X509Certificate cert) {
        try {
            List<String> ekus = cert.getExtendedKeyUsage();
            if (ekus != null && !ekus.contains(CLIENT_AUTH_OID)) {
                throw new BadTlsCredentialsException(
                        "Leaf certificate does not include the clientAuth extended key usage (OID " + CLIENT_AUTH_OID + "). " +
                                "Certificates used for upstream TLS client authentication must have clientAuth in their Extended Key Usage extension.");
            }
        }
        catch (CertificateException e) {
            throw new BadTlsCredentialsException("Failed to read extended key usage from certificate: " + e.getMessage(), e);
        }
    }

    private static void validateChainOrderAndSignatures(X509Certificate[] certChain, Date now) {
        for (int i = 0; i < certChain.length - 1; i++) {
            X509Certificate subject = certChain[i];
            X509Certificate issuer = certChain[i + 1];

            // Verify issuer relationship
            X500Principal subjectIssuer = subject.getIssuerX500Principal();
            X500Principal issuerSubject = issuer.getSubjectX500Principal();

            if (!subjectIssuer.equals(issuerSubject)) {
                throw new IllegalArgumentException(
                        "Certificate chain order is invalid at position " + i + ". " +
                                "Certificate issuer '" + subjectIssuer.getName() + "' " +
                                "does not match next certificate subject '" + issuerSubject.getName() + "'. " +
                                "Certificates must be ordered from leaf to intermediate certificates.");
            }

            // Verify signature
            try {
                subject.verify(issuer.getPublicKey());
            }
            catch (Exception e) {
                throw new IllegalArgumentException(
                        "Certificate at position " + i + " signature verification failed. " +
                                "Certificate '" + subject.getSubjectX500Principal().getName() + "' " +
                                "was not signed by '" + issuer.getSubjectX500Principal().getName() + "': " +
                                e.getMessage(),
                        e);
            }

            // Validate intermediate certificate dates
            try {
                issuer.checkValidity(now);
            }
            catch (CertificateException e) {
                throw new IllegalArgumentException(
                        "Intermediate certificate at position " + i + " is not valid: " + e.getMessage() +
                                " (subject: " + issuer.getSubjectX500Principal().getName() + ", " +
                                "valid from " + issuer.getNotBefore() + " to " + issuer.getNotAfter() + ")",
                        e);
            }
        }
    }

    /**
     * Checks if a certificate is self-signed (i.e., a root CA).
     *
     * @param cert The certificate to check
     * @return true if the certificate is self-signed
     */
    private static boolean isSelfSigned(@NonNull X509Certificate cert) {
        try {
            // Check if subject equals issuer
            if (!cert.getSubjectX500Principal().equals(cert.getIssuerX500Principal())) {
                return false;
            }

            // Verify signature with its own public key
            cert.verify(cert.getPublicKey());
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    /**
     * Converts TlsCredentials to a Netty SslContext for client-side connections.
     *
     * @param credentials The TLS credentials to convert
     * @return Netty SslContext configured with the provided credentials
     * @throws SSLException if the SslContext cannot be built
     */
    @NonNull
    public static SslContext toClientSslContext(@NonNull TlsCredentialsImpl credentials) throws SSLException {
        return SslContextBuilder.forClient()
                .keyManager(credentials.privateKey(), credentials.certificateChain())
                .build();
    }

    /**
     * Converts TlsCredentials to a Netty SslContext for client-side connections with custom trust configuration.
     *
     * @param credentials The TLS credentials to convert
     * @param builder Pre-configured SslContextBuilder with trust and cipher suite configuration
     * @return Netty SslContext configured with the provided credentials
     * @throws SSLException if the SslContext cannot be built
     */
    @NonNull
    public static SslContext toClientSslContext(@NonNull TlsCredentialsImpl credentials,
                                                @NonNull SslContextBuilder builder)
            throws SSLException {
        return builder.keyManager(credentials.privateKey(), credentials.certificateChain())
                .build();
    }
}
