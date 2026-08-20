/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Extracts names from an X.509 certificate, for use when deriving principals from a client's TLS certificate.
 */
public interface TlsCertificateExtractor extends Function<X509Certificate, Stream<String>> {

    /**
     * Returns an extractor yielding the certificate's subject distinguished name in RFC 2253 format.
     *
     * @return the subject DN extractor.
     */
    static TlsCertificateExtractor subject() {
        return x509Certificate -> Stream.of(x509Certificate.getSubjectX500Principal().getName());
    }

    /**
     * The types of subject alternative name that can be extracted from a certificate,
     * identified by their ASN.1 {@code GeneralName} tag value.
     */
    enum Asn1SanNameType {
        /** An RFC 822 (email address) name. */
        RFC822(1),
        /** A DNS name. */
        DNS(2),
        /** A directory (X.500 distinguished) name. */
        DIR_NAME(4),
        /** A uniform resource identifier. */
        URI(6),
        /** An IP address. */
        IP_ADDRESS(7);

        /** The ASN.1 {@code GeneralName} tag value of this name type. */
        public final int asn1Value;

        Asn1SanNameType(int asn1Value) {
            this.asn1Value = asn1Value;
        }
    }

    /**
     * Returns an extractor yielding the certificate's subject alternative names of the given type.
     * Certificates whose subject alternative names cannot be parsed yield no names.
     *
     * @param targetType the type of subject alternative name to extract.
     * @return the subject alternative name extractor.
     */
    static TlsCertificateExtractor san(Asn1SanNameType targetType) {
        return x509Certificate -> {
            try {
                return x509Certificate.getSubjectAlternativeNames().stream().flatMap(san -> {
                    Integer asn1SanType = (Integer) san.getFirst();
                    if (asn1SanType == targetType.asn1Value) {
                        return Stream.of((String) san.get(1));
                    }
                    else {
                        return Stream.empty();
                    }
                });
            }
            catch (CertificateParsingException e) {
                return Stream.empty();
            }
        };
    }
}
