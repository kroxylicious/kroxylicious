/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

/*
 * Adapted from Netty's io.netty.handler.ssl.PemReader for use without Netty dependencies.
 * Modifications:
 * - Replaced Netty ByteBuf with byte arrays
 * - Replaced Netty Base64 decoder with JDK Base64
 * - Added JCA integration to return PrivateKey and X509Certificate objects
 * - Added encrypted key detection and rejection
 */

package io.kroxylicious.testing.kms.tls;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Parses PEM-encoded private keys and certificates using only JDK APIs.
 * <p>
 * Forked from Netty's {@code io.netty.handler.ssl.PemReader} and adapted to work without Netty dependencies.
 * <p>
 * <strong>Limitations:</strong>
 * <ul>
 * <li>Only supports unencrypted PKCS#8 private keys ({@code -----BEGIN PRIVATE KEY-----})</li>
 * <li>Also supports unencrypted PKCS#1 RSA keys ({@code -----BEGIN RSA PRIVATE KEY-----})</li>
 * <li>Encrypted keys are NOT supported - use JKS/PKCS12 keystore format for encrypted keys</li>
 * </ul>
 */
final class PemUtils {

    private static final Pattern CERT_HEADER = Pattern.compile(
            "-+BEGIN\\s[^-\\r\\n]*CERTIFICATE[^-\\r\\n]*-+(?:\\s|\\r|\\n)+");
    private static final Pattern CERT_FOOTER = Pattern.compile(
            "-+END\\s[^-\\r\\n]*CERTIFICATE[^-\\r\\n]*-+(?:\\s|\\r|\\n)*");
    private static final Pattern KEY_HEADER = Pattern.compile(
            "-+BEGIN\\s[^-\\r\\n]*PRIVATE\\s+KEY[^-\\r\\n]*-+(?:\\s|\\r|\\n)+");
    private static final Pattern KEY_FOOTER = Pattern.compile(
            "-+END\\s[^-\\r\\n]*PRIVATE\\s+KEY[^-\\r\\n]*-+(?:\\s|\\r|\\n)*");
    private static final Pattern BODY = Pattern.compile("[a-z0-9+/=][a-z0-9+/=\\r\\n]*", Pattern.CASE_INSENSITIVE);

    private PemUtils() {
    }

    /**
     * Parses a PEM-encoded private key from a byte array.
     * <p>
     * <strong>Note:</strong> Only unencrypted PKCS#8 and PKCS#1 formats are supported.
     *
     * @param pemKeyBytes PEM-encoded private key bytes
     * @param password password for decrypting the private key (must be {@code null} as encryption is not supported)
     * @return the parsed PrivateKey
     * @throws IOException if the key cannot be parsed or is encrypted
     */
    @NonNull
    static PrivateKey parsePrivateKey(@NonNull byte[] pemKeyBytes, @Nullable char[] password) throws IOException {
        if (password != null) {
            throw new IOException(
                    "Encrypted private keys are not supported by this PEM parser. " +
                            "Use a keystore format (JKS/PKCS12) for encrypted keys, " +
                            "or convert to unencrypted PKCS#8: " +
                            "openssl pkcs8 -topk8 -nocrypt -in encrypted.pem -out unencrypted.pem");
        }

        byte[] keyBytes = readPrivateKey(new ByteArrayInputStream(pemKeyBytes));

        // First try as PKCS#8 (most common, works for all algorithms)
        PrivateKey key = tryParsePKCS8(keyBytes);
        if (key != null) {
            return key;
        }

        // Fall back to PKCS#1 RSA (legacy format from BouncyCastle JcaPEMWriter)
        key = tryParsePKCS1Rsa(keyBytes);
        if (key != null) {
            return key;
        }

        throw new IOException(
                "Failed to parse private key. Supported formats: PKCS#8 (all algorithms) and PKCS#1 (RSA only). " +
                        "PKCS#1 EC keys (BEGIN EC PRIVATE KEY) are not supported - convert to PKCS#8 using: " +
                        "openssl pkcs8 -topk8 -nocrypt -in ec_key.pem -out ec_key_pkcs8.pem");
    }

    private static PrivateKey tryParsePKCS8(byte[] keyBytes) {
        // Try common algorithms in order of likelihood
        String[] algorithms = { "RSA", "EC", "EdDSA", "DSA" };

        for (String algorithm : algorithms) {
            try {
                PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
                KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
                return keyFactory.generatePrivate(keySpec);
            }
            catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                // Try next algorithm
            }
        }
        return null;
    }

    private static PrivateKey tryParsePKCS1Rsa(byte[] keyBytes) {
        try {
            // PKCS#1 RSA keys need to be wrapped in PKCS#8 structure
            // This is a simple conversion that works for RSA keys
            byte[] pkcs8Bytes = convertPKCS1ToPKCS8(keyBytes);
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8Bytes);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(keySpec);
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * Converts PKCS#1 RSA private key to PKCS#8 format by wrapping it in the PKCS#8 structure.
     * PKCS#8 format is: SEQUENCE { version, algorithm, privateKey }
     * where privateKey is the PKCS#1 RSAPrivateKey wrapped as an OCTET STRING.
     */
    private static byte[] convertPKCS1ToPKCS8(byte[] pkcs1Bytes) throws IOException {
        // PKCS#8 wrapper for RSA:
        // SEQUENCE {
        // INTEGER 0 (version)
        // SEQUENCE { rsaEncryption OID, NULL }
        // OCTET STRING (containing the PKCS#1 key)
        // }

        int pkcs1Length = pkcs1Bytes.length;
        int totalLength = pkcs1Length + 22; // Fixed overhead for RSA wrapper

        ByteArrayOutputStream out = new ByteArrayOutputStream(totalLength + 4);

        // Outer SEQUENCE tag and length
        out.write(0x30); // SEQUENCE tag
        if (totalLength < 128) {
            out.write(totalLength);
        }
        else {
            out.write(0x82); // Long form: 2 length bytes
            out.write((totalLength >> 8) & 0xff);
            out.write(totalLength & 0xff);
        }

        // Version: INTEGER 0
        out.write(new byte[]{ 0x02, 0x01, 0x00 });

        // Algorithm: SEQUENCE { rsaEncryption OID, NULL }
        out.write(new byte[]{
                0x30, 0x0D, // SEQUENCE, length 13
                0x06, 0x09, // OID, length 9
                0x2A, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xF7, 0x0D, 0x01, 0x01, 0x01, // rsaEncryption OID: 1.2.840.113549.1.1.1
                0x05, 0x00 // NULL
        });

        // Private key: OCTET STRING containing PKCS#1 key
        out.write(0x04); // OCTET STRING tag
        if (pkcs1Length < 128) {
            out.write(pkcs1Length);
        }
        else if (pkcs1Length < 256) {
            out.write(0x81); // Long form: 1 length byte
            out.write(pkcs1Length);
        }
        else {
            out.write(0x82); // Long form: 2 length bytes
            out.write((pkcs1Length >> 8) & 0xff);
            out.write(pkcs1Length & 0xff);
        }
        out.write(pkcs1Bytes);

        return out.toByteArray();
    }

    /**
     * Parses a PEM-encoded X.509 certificate chain from a byte array.
     *
     * @param pemBytes PEM-encoded certificate bytes
     * @return array of parsed X509Certificates
     * @throws IOException if the certificates cannot be parsed
     */
    @NonNull
    static X509Certificate[] parseCertificateChain(@NonNull byte[] pemBytes) throws IOException {
        byte[][] certBytes = readCertificates(new ByteArrayInputStream(pemBytes));

        CertificateFactory cf;
        try {
            cf = CertificateFactory.getInstance("X.509");
        }
        catch (CertificateException e) {
            throw new IOException("Failed to get X.509 certificate factory", e);
        }

        List<X509Certificate> certificates = new ArrayList<>();
        for (byte[] certByte : certBytes) {
            try {
                X509Certificate cert = (X509Certificate) cf.generateCertificate(
                        new ByteArrayInputStream(certByte));
                certificates.add(cert);
            }
            catch (CertificateException e) {
                throw new IOException("Failed to parse certificate: " + e.getMessage(), e);
            }
        }

        return certificates.toArray(new X509Certificate[0]);
    }

    private static byte[][] readCertificates(InputStream in) throws IOException {
        String content = readContent(in);

        List<byte[]> certs = new ArrayList<>();
        Matcher m = CERT_HEADER.matcher(content);
        int start = 0;
        for (;;) {
            if (!m.find(start)) {
                break;
            }

            start = m.end();
            m.usePattern(BODY);
            if (!m.find(start)) {
                break;
            }

            String base64 = m.group(0).replaceAll("\\s", "");
            start = m.end();
            m.usePattern(CERT_FOOTER);
            if (!m.find(start)) {
                // Certificate is incomplete
                break;
            }

            byte[] der = Base64.getDecoder().decode(base64);
            certs.add(der);

            start = m.end();
            m.usePattern(CERT_HEADER);
        }

        if (certs.isEmpty()) {
            throw new IOException("found no certificates in input stream");
        }

        return certs.toArray(new byte[0][]);
    }

    private static byte[] readPrivateKey(InputStream in) throws IOException {
        String content = readContent(in);

        Matcher m = KEY_HEADER.matcher(content);
        if (!m.find()) {
            throw new IOException(
                    "could not find a PKCS #8 private key in input stream " +
                            "(see https://netty.io/wiki/sslcontextbuilder-and-private-key.html for more information)");
        }

        m.usePattern(BODY);
        if (!m.find()) {
            throw new IOException("could not find private key body");
        }

        String base64 = m.group(0).replaceAll("\\s", "");
        m.usePattern(KEY_FOOTER);
        if (!m.find()) {
            // Key is incomplete
            throw new IOException("could not find private key footer");
        }

        return Base64.getDecoder().decode(base64);
    }

    private static String readContent(InputStream in) throws IOException {
        return new String(in.readAllBytes(), StandardCharsets.US_ASCII);
    }

}
