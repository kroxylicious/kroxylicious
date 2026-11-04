/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.certificate;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Parses PEM-encoded private keys and certificate chains.
 */
public final class PemParser {

    private PemParser() {
    }

    /**
     * Parses a PEM-encoded private key from a byte array.
     *
     * @param pemKeyBytes PEM-encoded private key bytes
     * @return the parsed PrivateKey
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
     * @return the parsed PrivateKey
     * @throws IOException if the key cannot be parsed
     */
    @NonNull
    public static PrivateKey parsePrivateKey(@NonNull byte[] pemKeyBytes,
                                             @Nullable char[] password)
            throws IOException {
        try (Reader reader = new InputStreamReader(new java.io.ByteArrayInputStream(pemKeyBytes), StandardCharsets.US_ASCII);
                PEMParser pemParser = new PEMParser(reader)) {

            Object object = pemParser.readObject();
            if (object == null) {
                throw new IOException("No private key found in PEM data");
            }

            JcaPEMKeyConverter converter = new JcaPEMKeyConverter();

            if (object instanceof PEMKeyPair pemKeyPair) {
                return converter.getKeyPair(pemKeyPair).getPrivate();
            }
            else if (object instanceof PrivateKeyInfo privateKeyInfo) {
                return converter.getPrivateKey(privateKeyInfo);
            }
            else if (object instanceof PKCS8EncryptedPrivateKeyInfo encryptedInfo) {
                if (password == null) {
                    throw new IOException("Encrypted private key requires a password");
                }
                PrivateKeyInfo decrypted = encryptedInfo.decryptPrivateKeyInfo(
                        new JceOpenSSLPKCS8DecryptorProviderBuilder()
                                .setProvider(new BouncyCastleProvider())
                                .build(password));
                return converter.getPrivateKey(decrypted);
            }
            else if (object instanceof PEMEncryptedKeyPair encryptedKeyPair) {
                if (password == null) {
                    throw new IOException("Encrypted private key requires a password");
                }
                PEMKeyPair decrypted = encryptedKeyPair.decryptKeyPair(
                        new JcePEMDecryptorProviderBuilder()
                                .setProvider(new BouncyCastleProvider())
                                .build(password));
                return converter.getKeyPair(decrypted).getPrivate();
            }
            else {
                throw new IOException("Unexpected PEM object type: " + object.getClass().getName());
            }
        }
        catch (IOException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IOException("Failed to parse private key: " + e.getMessage(), e);
        }
    }

    /**
     * Parses a PEM-encoded certificate chain from a byte array.
     *
     * @param pemBytes PEM-encoded certificate bytes
     * @return array of parsed X509Certificates
     * @throws IOException if the certificates cannot be parsed
     */
    @NonNull
    public static X509Certificate[] parseCertificateChain(@NonNull byte[] pemBytes) throws IOException {
        try (Reader reader = new InputStreamReader(new java.io.ByteArrayInputStream(pemBytes), StandardCharsets.US_ASCII);
                PEMParser pemParser = new PEMParser(reader)) {

            JcaX509CertificateConverter converter = new JcaX509CertificateConverter();
            List<X509Certificate> certificates = new ArrayList<>();

            Object object;
            while ((object = pemParser.readObject()) != null) {
                if (object instanceof X509CertificateHolder holder) {
                    certificates.add(converter.getCertificate(holder));
                }
            }

            if (certificates.isEmpty()) {
                throw new IOException("No certificates found in PEM data");
            }

            return certificates.toArray(new X509Certificate[0]);
        }
        catch (IOException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IOException("Failed to parse certificate chain: " + e.getMessage(), e);
        }
    }
}
