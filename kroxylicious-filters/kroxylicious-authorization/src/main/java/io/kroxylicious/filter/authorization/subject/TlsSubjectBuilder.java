/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization.subject;

import java.security.cert.CertificateParsingException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.authorizer.service.Principal;
import io.kroxylicious.authorizer.service.Subject;
import io.kroxylicious.proxy.tls.ClientTlsContext;

public class TlsSubjectBuilder implements ClientSubjectBuilder {

    public static final UnaryOperator<String> DEFAULT = UnaryOperator.identity();
    private final String subjectNameFormat;
    private final UnaryOperator<String> subjectMappingRules;
    private final UnaryOperator<String> sanRfc822NameMappingRules;
    private final UnaryOperator<String> sanDnsNameMappingRules;
    private final UnaryOperator<String> sanDirectoryNameMappingRules;
    private final UnaryOperator<String> sanUriMappingRules;
    private final UnaryOperator<String> sanIpAddressMappingRules;

    public TlsSubjectBuilder(String subjectNameFormat,
                             UnaryOperator<String> subjectMappingRules,
                             UnaryOperator<String> sanRfc822NameMappingRules,
                             UnaryOperator<String> sanDnsNameMappingRules,
                             UnaryOperator<String> sanDirectoryNameMappingRules,
                             UnaryOperator<String> sanUriMappingRules,
                             UnaryOperator<String> sanIpAddressMappingRules) {
        this.sanDirectoryNameMappingRules = sanDirectoryNameMappingRules;
        this.subjectNameFormat = subjectNameFormat;
        this.subjectMappingRules = subjectMappingRules;
        this.sanRfc822NameMappingRules = sanRfc822NameMappingRules;
        this.sanDnsNameMappingRules = sanDnsNameMappingRules;
        this.sanUriMappingRules = sanUriMappingRules;
        this.sanIpAddressMappingRules = sanIpAddressMappingRules;
    }

    @Override
    public CompletionStage<Subject> buildSubject(Context context) {

        var s = context.clientTlsContext().flatMap(
                ClientTlsContext::clientCertificate).map(
                cert -> {
                    Stream<String> names = null;
                    if (this.sanDirectoryNameMappingRules != null
                        || this.sanDnsNameMappingRules != null
                        || this.sanUriMappingRules != null
                        || this.sanIpAddressMappingRules != null
                        || this.sanRfc822NameMappingRules != null) {
                        try {
                            var sans = cert.getSubjectAlternativeNames();
                            if (sans != null) {
                                names = sans.stream().flatMap(san -> {
                                    return Optional.ofNullable(switch ((Integer) san.get(0)) {
                                        case 0 -> null; //otherName => skip
                                        case 1 -> sanRfc822NameMappingRules.apply((String) san.get(1)); // i.e. email address
                                        case 2 -> sanDnsNameMappingRules.apply((String) san.get(1));
                                        case 3 -> null; // x400Address => skip
                                        case 4 -> sanDirectoryNameMappingRules.apply((String) san.get(1));
                                        case 5 -> null; // ediPartyName => skip
                                        case 6 -> sanUriMappingRules.apply((String) san.get(1));
                                        case 7 -> sanIpAddressMappingRules.apply((String) san.get(1));
                                        case 8 -> null; // registeredID => skip
                                        default -> null; // ??? => skip
                                    }).stream();
                                });
                            }
                        }
                        catch (CertificateParsingException e) {
                            // TODO log
                        }
                    }
                    String nameType = Optional.ofNullable(subjectNameFormat).orElse(javax.security.auth.x500.X500Principal.RFC2253);

                    var subjectName = Optional.ofNullable(subjectMappingRules).map(rule -> rule.apply(cert.getSubjectX500Principal().getName(nameType))).stream();

                    if (names != null) {
                        names = Stream.concat(subjectName, names);
                    }
                    else {
                        names = subjectName;
                    }

                    Set<Principal> principals = names.map(User::new).collect(Collectors.toSet());
                    return principals.isEmpty() ? Subject.ANONYMOUS : new Subject(principals);
                })
                .orElse(Subject.ANONYMOUS);
        return CompletableFuture.completedStage(s);
    }
}
