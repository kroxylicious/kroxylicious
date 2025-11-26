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

public interface TlsCertificateExtractor extends Function<X509Certificate, Stream<String>> {

    static TlsCertificateExtractor subject() {
        return new TlsCertificateExtractor() {
            @Override
            public Stream<String> apply(X509Certificate x509Certificate) {
                return Stream.of(x509Certificate.getSubjectX500Principal().getName());
            }
        };
    }

    enum Asn1SanNameType {
        RFC822(1),
        DNS(2),
        DIR_NAME(4),
        URI(6),
        IP_ADDRESS(7);

        public final int asn1Value;

        Asn1SanNameType(int asn1Value) {
            this.asn1Value = asn1Value;
        }
    }

    static TlsCertificateExtractor san(Asn1SanNameType targetType) {
        return new TlsCertificateExtractor() {
            @Override
            public Stream<String> apply(X509Certificate x509Certificate) {
                try {
                    return x509Certificate.getSubjectAlternativeNames().stream().flatMap(san -> {
                        Integer asn1SanType = (Integer) san.get(0);
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
            }
        };
    }
}
