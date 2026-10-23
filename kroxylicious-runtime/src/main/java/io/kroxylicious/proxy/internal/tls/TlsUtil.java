/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;

import javax.security.auth.x500.X500Principal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.KeyProvider;
import io.kroxylicious.proxy.config.tls.KeyProviderVisitor;
import io.kroxylicious.proxy.config.tls.KeyStore;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Utility class for TLS credential validation.
 */
@SuppressWarnings("java:S1192") // ignore dupe string literals is due to logger keys
public class TlsUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(TlsUtil.class);

    private TlsUtil() {
        // Utility class
    }

    /**
     * Validates that a private key matches the public key in a certificate.
     *
     * @param privateKey The private key to validate
     * @param certificate The certificate containing the public key
     * @throws BadTlsCredentialsException if the keys don't match
     */
    @VisibleForTesting
    static void validateKeyAndCertMatch(@NonNull final PrivateKey privateKey, @NonNull final X509Certificate certificate) {
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
            case "DSA" -> {
                LOGGER.atWarn().log("DSA is deprecated in TLS 1.3 and usage is discouraged");
                validateDsaKeyMatch(privateKey, publicKey);
            }
            default -> LOGGER.atDebug()
                    .addKeyValue("algorithm", algorithm)
                    .log("Key-certificate matching validated via algorithm check");
        }

        LOGGER.atDebug()
                .addKeyValue("algorithm", algorithm)
                .log("Private key and certificate public key validated successfully");
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

        LOGGER.atDebug()
                .log("RSA key modulus match validated");
    }

    /**
     * Validates that an EC private key matches an EC public key using a sign-then-verify round-trip.
     *
     * @param privateKey The EC private key
     * @param publicKey The EC public key
     * @throws BadTlsCredentialsException if the keys don't match
     */
    private static void validateEcKeyMatch(@NonNull PrivateKey privateKey, @NonNull PublicKey publicKey) {
        if (!(privateKey instanceof ECPrivateKey)) {
            throw new BadTlsCredentialsException("Expected ECPrivateKey but got " + privateKey.getClass().getName());
        }
        if (!(publicKey instanceof ECPublicKey)) {
            throw new BadTlsCredentialsException("Expected ECPublicKey but got " + publicKey.getClass().getName());
        }

        // Verify key correspondence via sign-then-verify round-trip
        try {
            java.security.Signature sig = java.security.Signature.getInstance("SHA256withECDSA");
            byte[] challenge = "kroxylicious-ec-key-validation".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            sig.initSign(privateKey);
            sig.update(challenge);
            byte[] signature = sig.sign();

            sig.initVerify(publicKey);
            sig.update(challenge);
            if (!sig.verify(signature)) {
                throw new BadTlsCredentialsException(
                        "EC private key does not correspond to certificate public key. " +
                                "Signature verification failed.");
            }
        }
        catch (BadTlsCredentialsException e) {
            throw e;
        }
        catch (Exception e) {
            throw new BadTlsCredentialsException(
                    "Failed to validate EC key correspondence: " + e.getMessage(), e);
        }

        LOGGER.atDebug()
                .log("EC key-certificate match validated via signature verification");
    }

    /**
     * Validates that a DSA private key matches a DSA public key using a sign-then-verify round-trip.
     *
     * @param privateKey The DSA private key
     * @param publicKey The DSA public key
     * @throws BadTlsCredentialsException if the keys don't match
     */
    private static void validateDsaKeyMatch(@NonNull PrivateKey privateKey, @NonNull PublicKey publicKey) {
        try {
            java.security.Signature sig = java.security.Signature.getInstance("SHA256withDSA");
            byte[] challenge = "kroxylicious-dsa-key-validation".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            sig.initSign(privateKey);
            sig.update(challenge);
            byte[] signature = sig.sign();

            sig.initVerify(publicKey);
            sig.update(challenge);
            if (!sig.verify(signature)) {
                throw new BadTlsCredentialsException(
                        "DSA private key does not correspond to certificate public key. " +
                                "Signature verification failed.");
            }
        }
        catch (BadTlsCredentialsException e) {
            throw e;
        }
        catch (Exception e) {
            throw new BadTlsCredentialsException(
                    "Failed to validate DSA key correspondence: " + e.getMessage(), e);
        }

        LOGGER.atDebug()
                .log("DSA key-certificate match validated via signature verification");
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

        LOGGER.atDebug()
                .log("Certificate chain validation passed: {} certificates in chain", certChain.length);
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
     * Validates that the certificate(s) in the KeyProvider were derived from the corresponding private key.
     * Where a key store contains multiple key entries, every recoverable key entry is validated, as the
     * runtime KeyManager may serve any of them depending on the handshake (key type, acceptable issuers, SNI).
     *
     * @param keyProvider the key provider to validate
     * @return Optional.empty() if validation could not be performed (unsupported algorithm, I/O error, etc.),
     *         Optional.of(true) if the certificate(s) match the key(s),
     *         Optional.of(false) if a certificate does not match its key
     */
    public static Optional<Boolean> validateCertificateKeyPair(final KeyProvider keyProvider) {
        try {
            final List<KeyAndCert> keyAndCerts = keyProvider.accept(new KeyProviderExtractionVisitor());
            for (final KeyAndCert keyAndCert : keyAndCerts) {
                validateKeyAndCertMatch(keyAndCert.privateKey(), keyAndCert.certificate());
            }
            return Optional.of(true);
        }
        catch (final BadTlsCredentialsException e) {
            // thrown when keys don't match
            LOGGER.atDebug()
                    .setCause(e)
                    .log("Certificate and private key do not match");
            return Optional.of(false);
        }
        catch (final RuntimeException e) {
            // Parsing failed (SslContextBuildException), unsupported algorithm, or other error - cannot validate
            LOGGER.atWarn()
                    .setCause(LOGGER.isDebugEnabled() ? e : null)
                    .addKeyValue("error", e.getMessage())
                    .log(LOGGER.isDebugEnabled()
                            ? "Certificate-key validation could not be performed"
                            : "Certificate-key validation could not be performed, increase log level to DEBUG for stacktrace");
            return Optional.empty();
        }
    }

    /**
     * Visitor that extracts the PrivateKey and X509Certificate pairs from a KeyProvider.
     * A KeyPair or PEM key store yields a single pair; a JKS/PKCS12 key store may yield several.
     */
    private static class KeyProviderExtractionVisitor implements KeyProviderVisitor<List<KeyAndCert>> {

        @Override
        // Paths come from the trusted proxy TLS configuration, not from untrusted input
        @SuppressFBWarnings("PATH_TRAVERSAL_IN")
        public List<KeyAndCert> visit(final KeyPair keyPair) {
            try {
                final byte[] keyBytes = Files.readAllBytes(Paths.get(keyPair.privateKeyFile()));
                final byte[] certBytes = Files.readAllBytes(Paths.get(keyPair.certificateFile()));
                return extractFromPem(keyBytes, certBytes);
            }
            catch (final IOException e) {
                throw new SslContextBuildException("Failed to extract key and certificate from KeyPair", e);
            }
        }

        @Override
        // Paths come from the trusted proxy TLS configuration, not from untrusted input
        @SuppressFBWarnings("PATH_TRAVERSAL_IN")
        public List<KeyAndCert> visit(final KeyStore keyStore) {
            try {
                if (keyStore.isPemType()) {
                    // a PEM key store holds the key and certificate in the same file
                    final byte[] pemBytes = Files.readAllBytes(Paths.get(keyStore.storeFile()));
                    return extractFromPem(pemBytes, pemBytes);
                }
                else {
                    // JKS/PKCS12 format
                    return extractFromKeyStore(keyStore);
                }
            }
            catch (final IOException | GeneralSecurityException e) {
                throw new SslContextBuildException("Failed to extract key and certificate from KeyStore", e);
            }
        }
    }

    /**
     * Extracts the private key and leaf certificate from PEM-encoded bytes using {@link PemUtils}. The key
     * and certificate may come from the same file (a PEM key store) or from separate files (a key pair).
     *
     * @param keyPem the PEM bytes containing the private key
     * @param certPem the PEM bytes containing the certificate
     * @return the key and leaf certificate, or an empty list if the key is encrypted and so cannot be parsed here
     * @throws IOException if the key or certificate cannot be parsed
     */
    // S3398: kept as a class-scoped parsing helper alongside the other credential-extraction utilities rather than nested in the visitor
    @SuppressWarnings("java:S3398")
    private static List<KeyAndCert> extractFromPem(final byte[] keyPem, final byte[] certPem) throws IOException {
        // Encrypted PKCS#8 keys are decrypted by Netty (using the JDK) when constructing the endpoint; we cannot pre-validate them here
        if (isEncryptedPem(keyPem)) {
            return List.of();
        }
        final PrivateKey privateKey = PemUtils.parsePrivateKey(keyPem, null);
        final X509Certificate[] certs = PemUtils.parseCertificateChain(certPem);
        return List.of(new KeyAndCert(privateKey, certs[0]));
    }

    /**
     * Extracts the private key and leaf certificate of every key entry from a Java KeyStore (JKS/PKCS12).
     * All key entries are returned rather than only the first, as the runtime KeyManager may serve any of
     * them depending on the handshake. A key entry whose key cannot be recovered with the configured
     * password is skipped, mirroring the leniency of the runtime KeyManagerFactory.
     *
     * @param keyStoreConfig the keystore configuration
     * @return the extracted key and certificate pairs
     * @throws IOException if the keystore cannot be read
     * @throws GeneralSecurityException if the keystore cannot be loaded, key extraction fails, or it contains no recoverable key entries
     */
    // Path comes from the trusted proxy TLS configuration, not from untrusted input
    // S3398: kept as a class-scoped parsing helper alongside the other credential-extraction utilities rather than nested in the visitor
    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    @SuppressWarnings("java:S3398")
    private static List<KeyAndCert> extractFromKeyStore(final KeyStore keyStoreConfig)
            throws IOException, GeneralSecurityException {
        final char[] storePass = keyStoreConfig.storePasswordProvider() != null
                ? keyStoreConfig.storePasswordProvider().getProvidedPassword().toCharArray()
                : null;
        final char[] keyPass = keyStoreConfig.keyPasswordProvider() != null
                ? keyStoreConfig.keyPasswordProvider().getProvidedPassword().toCharArray()
                : storePass; // Default to store password if key password not specified

        final java.security.KeyStore ks = java.security.KeyStore.getInstance(keyStoreConfig.getType());
        try (FileInputStream fis = new FileInputStream(keyStoreConfig.storeFile())) {
            ks.load(fis, storePass);
        }

        final List<KeyAndCert> keyAndCerts = new ArrayList<>();
        final Enumeration<String> aliases = ks.aliases();
        while (aliases.hasMoreElements()) {
            extractKeyEntry(ks, aliases.nextElement(), keyPass).ifPresent(keyAndCerts::add);
        }
        if (keyAndCerts.isEmpty()) {
            throw new KeyStoreException("No key entry found in keystore: " + keyStoreConfig.storeFile());
        }
        return keyAndCerts;
    }

    /**
     * Extracts the private key and leaf certificate for a single keystore alias, if it is a key
     * entry whose key can be recovered with the given password and whose certificate is an X.509
     * certificate. A key entry that cannot be recovered is skipped, mirroring the leniency of the
     * runtime KeyManagerFactory.
     *
     * @param ks the loaded keystore
     * @param alias the alias to inspect
     * @param keyPass the password used to recover the key
     * @return the key and certificate, or empty if the alias is not a usable X.509 key entry
     * @throws GeneralSecurityException if the keystore cannot be queried
     */
    private static Optional<KeyAndCert> extractKeyEntry(final java.security.KeyStore ks, final String alias, final char[] keyPass)
            throws GeneralSecurityException {
        if (!ks.isKeyEntry(alias)) {
            return Optional.empty();
        }
        final Key key;
        try {
            key = ks.getKey(alias, keyPass);
        }
        catch (final UnrecoverableKeyException e) {
            LOGGER.atDebug()
                    .setCause(e)
                    .addKeyValue("alias", alias)
                    .log("Skipping key entry that could not be recovered with the configured password");
            return Optional.empty();
        }
        if (key instanceof PrivateKey pk) {
            final Certificate cert = ks.getCertificate(alias);
            if (cert instanceof X509Certificate x509) {
                return Optional.of(new KeyAndCert(pk, x509));
            }
            LOGGER.atDebug()
                    .addKeyValue("alias", alias)
                    .addKeyValue("certificateType", cert == null ? "none" : cert.getType())
                    .log("Skipping key entry whose certificate is not an X.509 certificate");
        }
        return Optional.empty();
    }

    /**
     * Holds a private key and its corresponding certificate.
     */
    private record KeyAndCert(PrivateKey privateKey, X509Certificate certificate) {}

    /**
     * Returns true if the PEM contains an encrypted PKCS#8 private key. Such keys are decrypted by
     * Netty (using the JDK) when the endpoint is built, so they cannot be pre-validated here and are skipped.
     *
     * @param pemBytes PEM-encoded private key data
     * @return true if the key is an encrypted PKCS#8 key
     */
    private static boolean isEncryptedPem(final byte[] pemBytes) {
        return new String(pemBytes, StandardCharsets.UTF_8).contains("-----BEGIN ENCRYPTED PRIVATE KEY-----");
    }
}
