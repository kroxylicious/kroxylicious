/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilderService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = DefaultTransportSubjectBuilderService.Config.class)
public class DefaultTransportSubjectBuilderService implements TransportSubjectBuilderService<DefaultTransportSubjectBuilderService.Config> {

    public static final String CLIENT_TLS_SUBJECT = "clientTlsSubject";
    public static final String CLIENT_TLS_SAN_RFC822_NAME = "clientTlsSanRfc822Name";
    public static final String CLIENT_TLS_SAN_DIR_NAME = "clientTlsSanDirName";
    public static final String CLIENT_TLS_SAN_DNS_NAME = "clientTlsSanDnsName";
    public static final String CLIENT_TLS_SAN_URI = "clientTlsSanUri";
    public static final String CLIENT_TLS_SAN_IP_ADDRESS = "clientTlsSanIpAddress";
    public static final String ELSE_IDENTITY = "identity";
    public static final String ELSE_ANONYMOUS = "anonymous";

    /*
     * subjectBuilder:
     * - type: DefaultSubjectBuilder
     * config:
     * addPrincipals:
     * - from: clientTlsSubject # a singleton or optional
     * map:
     * - sedLike: #CN=(.*?),.*#$1#
     * - else: identity
     * principalFactory: UserFactory
     * - from: clientTlsSubject
     * map:
     * - sedLike: #.*,OU=(.*?).*#$1#
     * - else: anonymous
     * principalFactory: RoleFactory
     * - from: LdapMemerOf # multi valued
     * map:
     * - sedLike: #.*,OU=(.*?).*#$1#
     * - else: anonymous
     */
    public record Config(List<PrincipalAdderConf> addPrincipals) {

    }

    /**
     * Configuration for a principal adder, which is responsible for contributing zero or more principals to the subject.
     * @param from Names a function for extracting a string value from a {@link TransportSubjectBuilder.Context}.
     * @param map An optional list of mappings to apply to the `from`-extracted string.
     * @param principalFactory The name of a {@link PrincipalFactory} implementation class.
     */
    public record PrincipalAdderConf(@JsonProperty(required = true) String from,
                                     @Nullable List<Map> map,
                                     @JsonProperty(required = true) String principalFactory) {
        public PrincipalAdderConf {
            // call methods for validation side-effect
            buildExtractor(from);
            buildMappingRules(map);
            buildPrincipalFactory(principalFactory);
        }
    }

    record Map(@Nullable String replaceMatch,
               @JsonProperty("else") @Nullable String else_) {
        Map {
            if (replaceMatch != null) {
                if (else_ != null) {
                    throw new IllegalArgumentException("`replaceMatch` and `else` are mutually exclusive.");
                }
                new ReplaceMatchMappingRule(replaceMatch);
            }
            else if (else_ == null) {
                throw new IllegalArgumentException("Exactly one of `replaceMatch` and `else` are required.");
            }
            else if (!else_.equals(ELSE_IDENTITY)
                    && !else_.equals(ELSE_ANONYMOUS)) {
                throw new IllegalArgumentException("`else` can only take the value 'identity' or 'anonymous'.");
            }
        }
    }

    List<PrincipalAdder> adders;

    @Override
    public void initialize(@Nullable Config config) {
        adders = Plugins.requireConfig(this, config).addPrincipals().stream()
                .map(addConf -> new PrincipalAdder(buildExtractor(addConf.from()),
                        buildMappingRules(addConf.map()),
                        buildPrincipalFactory(addConf.principalFactory())))
                .toList();
    }

    private static PrincipalFactory buildPrincipalFactory(String principalFactory) {
        return ServiceLoader.load(PrincipalFactory.class).stream()
                .filter(provider -> provider.type().getName().equals(principalFactory))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("`principalFactory` '%s' not found.".formatted(principalFactory)))
                .get();
    }

    @NonNull
    private static List<MappingRule> buildMappingRules(List<Map> maps) {
        if (maps == null || maps.isEmpty()) {
            return List.of(new IdentityMappingRule());
        }
        int firstElseIndex = -1;
        int numElses = 0;
        for (int i = 0; i < maps.size(); i++) {
            Map m = maps.get(i);

            if (m.else_() != null) {
                numElses++;
                if (firstElseIndex == -1) {
                    firstElseIndex = i;
                }
            }
        }
        if (numElses > 1 || (firstElseIndex != -1 && firstElseIndex < maps.size() - 1)) {
            throw new IllegalArgumentException("An `else` mapping may only occur as the last element of `map`.");
        }
        return maps.stream().map(map -> {
            if (map.replaceMatch() != null) {
                return new ReplaceMatchMappingRule(map.replaceMatch());
            }
            else if (ELSE_IDENTITY.equals(map.else_())) {
                return new IdentityMappingRule();
            }
            else if (ELSE_ANONYMOUS.equals(map.else_())) {
                return (MappingRule) s -> Optional.empty();
            }
            else {
                throw new IllegalArgumentException("Unknown `else` map '%s', supported values are: '%s', '%s'."
                        .formatted(map.else_(), ELSE_IDENTITY, ELSE_ANONYMOUS));
            }
        }).toList();
    }

    @NonNull
    private static Function<Object, Stream<String>> buildExtractor(String from) {
        return switch (from) {
            case CLIENT_TLS_SUBJECT -> getContextStreamFunction(TlsCertificateExtractor.subject());
            case CLIENT_TLS_SAN_RFC822_NAME -> getContextStreamFunction(TlsCertificateExtractor.san(TlsCertificateExtractor.Asn1SanNameType.RFC822));
            case CLIENT_TLS_SAN_DIR_NAME -> getContextStreamFunction(TlsCertificateExtractor.san(TlsCertificateExtractor.Asn1SanNameType.DIR_NAME));
            case CLIENT_TLS_SAN_DNS_NAME -> getContextStreamFunction(TlsCertificateExtractor.san(TlsCertificateExtractor.Asn1SanNameType.DNS));
            case CLIENT_TLS_SAN_URI -> getContextStreamFunction(TlsCertificateExtractor.san(TlsCertificateExtractor.Asn1SanNameType.URI));
            case CLIENT_TLS_SAN_IP_ADDRESS -> getContextStreamFunction(TlsCertificateExtractor.san(TlsCertificateExtractor.Asn1SanNameType.IP_ADDRESS));
            default -> throw new IllegalArgumentException("Unknown `from` '%s', supported values are: %s."
                    .formatted(from,
                            Stream.of(CLIENT_TLS_SUBJECT,
                                    CLIENT_TLS_SAN_RFC822_NAME,
                                    CLIENT_TLS_SAN_DIR_NAME,
                                    CLIENT_TLS_SAN_DNS_NAME,
                                    CLIENT_TLS_SAN_URI,
                                    CLIENT_TLS_SAN_IP_ADDRESS).map(s -> '\'' + s + '\'')
                                    .collect(Collectors.joining(", "))));
        };
    }

    @NonNull
    private static Function<Object, Stream<String>> getContextStreamFunction(TlsCertificateExtractor extractor) {
        return context -> ((TransportSubjectBuilder.Context) context).clientTlsContext().stream()
                .flatMap(clientCertificate -> clientCertificate.clientCertificate().stream())
                .flatMap(extractor);
    }

    @Override
    public TransportSubjectBuilder build() {
        return new DefaultSubjectBuilder(adders);
    }

    @Override
    public void close() {
    }

}
