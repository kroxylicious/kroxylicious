/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization.subject;

import java.security.cert.CertificateParsingException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.authorizer.service.Subject;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

public class TlsSubjectBuilder implements ClientSubjectBuilder {

    public static final UnaryOperator<String> DEFAULT = UnaryOperator.identity();
    String subjectNameFormat;
    UnaryOperator<String> subjectMappingRules = DEFAULT;
    UnaryOperator<String> sanRfc822NameMappingRules = DEFAULT;
    UnaryOperator<String> sanDnsNameMappingRules = DEFAULT;
    UnaryOperator<String> sanDirectoryNameMappingRules = DEFAULT;
    UnaryOperator<String> sanUriMappingRules = DEFAULT;
    UnaryOperator<String> sanIpAddressMappingRules = DEFAULT;

    record Config(
            @Nullable String subjectNameFormat,
            @Nullable List<String> subjectMappingRules,
            @Nullable List<String> sanRfc822NameMappingRules,
            @Nullable List<String> sanDnsNameMappingRules,
            @Nullable List<String> sanDirectoryNameMappingRules,
            @Nullable List<String> sanUriMappingRules,
            @Nullable List<String> sanIpAddressMappingRules
    ) { }

    class RegexMappingRule {
        static final Pattern RULE_PATTERN = Pattern.compile("^(?<pattern>.*?)/(?<replacement>.*?)/(?<flags>[LU])$");
        private final Pattern pattern;
        private final String replacement;
        private final UnaryOperator<String> flags;

        RegexMappingRule(String rule) {
            Matcher matcher = RULE_PATTERN.matcher(rule);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid rule: " + rule);
            }
            this.pattern = Pattern.compile(matcher.group("pattern"));
            this.replacement = matcher.group("replacement");
            this.flags = switch (matcher.group("flags")) {
                case "L" -> String::toLowerCase;
                case "U" -> String::toUpperCase;
                case "" -> DEFAULT;
                default -> throw new IllegalArgumentException("Invalid rule: " + rule);
            };
        }
    }

    class MappingRules implements UnaryOperator<String> {
        private final List<RegexMappingRule> rules;

        MappingRules(List<String> mappingRules) {
            this.rules = mappingRules.stream().map(RegexMappingRule::new).toList();
        }

        @Override
        public String apply(String s) {
            for (var fn : this.rules) {
                var matcher = fn.pattern.matcher(s);
                if (matcher.matches()) {
                    return fn.flags.apply(matcher.replaceAll(fn.replacement));
                }
            }
            return s;
        }
    }


    @Override
    public CompletionStage<Subject> buildSubject(Context context) {

        var s = context.clientTlsContext().flatMap(
                ClientTlsContext::clientCertificate).map(
                cert -> {
                    if (this.sanDirectoryNameMappingRules != null
                        || this.sanDnsNameMappingRules != null
                        || this.sanUriMappingRules != null
                        || this.sanIpAddressMappingRules != null
                        || this.sanRfc822NameMappingRules != null) {
                        try {
                            var sans = cert.getSubjectAlternativeNames();
                            if (sans != null) {
                                sans.stream().flatMap(san -> {
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
                                }).collect(Collectors.toSet());
                            }
                        }
                        catch (CertificateParsingException e) {
                            // TODO log
                        }
                    }
                    String nameType = Optional.ofNullable(subjectNameFormat).orElse(javax.security.auth.x500.X500Principal.RFC2253);
                    return new Subject(Set.of(new User(subjectMappingRules.apply(cert.getSubjectX500Principal().getName(nameType)))));
                })
                .orElse(Subject.ANONYMOUS);
        return CompletableFuture.completedStage(s);
    }
}
