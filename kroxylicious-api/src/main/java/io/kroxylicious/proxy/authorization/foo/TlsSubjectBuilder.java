/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization.foo;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.authorization.Subject;
import io.kroxylicious.proxy.authorization.SubjectBuilder;

public class TlsSubjectBuilder implements SubjectBuilder {

    @Override
    public CompletionStage<Subject> buildSubject(Context context) {

        var s = context.x509Certificate().map(
                cert -> {
//                    try {
//                        var sans = cert.getSubjectAlternativeNames();
//                        if (sans != null) {
//                            for (var san : sans) {
//                                switch ((Integer) san.get(0)) {
//                                    case 0 -> otherName(san.get(1));
//                                    case 1 -> rfc822Name((String) san.get(1));
//                                    case 2 -> dNSName((String) san.get(1));
//                                    case 3 -> x400Address((String) san.get(1));
//                                    case 4 -> directoryName((String) san.get(1));
//                                    case 5 -> ediPartyName(san.get(1));
//                                    case 6 -> uniformResourceIdentifier((String) san.get(1));
//                                    case 7 -> iPAddress((String) san.get(1));
//                                    case 8 -> registeredID(san.get(1));
//                                }
//
//                            }
//                        }
//                    } catch (CertificateParsingException e) {
//                        // TODO log
//                    }
                    return new Subject(Set.of(new X500Principal(cert.getSubjectX500Principal())));
                })
                .orElse(Subject.ANONYMOUS);
        return CompletableFuture.completedStage(s);
    }
}
