/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import edu.umd.cs.findbugs.annotations.NonNull;

public class CertificateGenerator {

    public static KeyPair generateRsaKeyPair() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(1024, new SecureRandom());
            return generator.generateKeyPair();
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toRsaPrivateKeyPem(KeyPair pair) {
        try {
            return toPemString(pair.getPrivate());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NonNull
    private static String toPemString(Object obj) throws IOException {
        try (StringWriter out = new StringWriter()) {
            try (JcaPEMWriter pemWriter = new JcaPEMWriter(out)) {
                pemWriter.writeObject(obj);
            }
            return out.toString();
        }
    }

    public static String toCertPem(X509Certificate certificate) {
        try {
            return toPemString(certificate);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static X509Certificate generateSelfSignedX509Certificate(KeyPair pair) {
        try {
            var subPubKeyInfo = SubjectPublicKeyInfo.getInstance(pair.getPublic().getEncoded());
            var now = Instant.now();
            var validFrom = Date.from(now);
            var validTo = Date.from(now.plus(Duration.ofDays(9999)));
            var certBuilder = new X509v3CertificateBuilder(
                    new X500Name("CN=localhost"),
                    BigInteger.ONE,
                    validFrom,
                    validTo,
                    new X500Name("CN=localhost"),
                    subPubKeyInfo);
            var signer = new JcaContentSignerBuilder("SHA256WithRSA")
                    .setProvider(new BouncyCastleProvider())
                    .build(pair.getPrivate());
            X509CertificateHolder holder = certBuilder.build(signer);
            JcaX509CertificateConverter converter = new JcaX509CertificateConverter();
            return converter.getCertificate(holder);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
