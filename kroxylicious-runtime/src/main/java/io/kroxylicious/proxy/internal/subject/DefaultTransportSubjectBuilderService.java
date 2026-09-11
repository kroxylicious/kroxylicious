/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.proxy.authentication.PrincipalFactory;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilderService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The default {@link TransportSubjectBuilderService} plugin. It builds {@link io.kroxylicious.proxy.authentication.Subject}s
 * from the client's TLS certificate by extracting names (subject DN or subject alternative names),
 * transforming them through configured mapping rules and turning the results into principals.
 */
@Plugin(configType = DefaultTransportSubjectBuilderService.Config.class)
public class DefaultTransportSubjectBuilderService implements TransportSubjectBuilderService<DefaultTransportSubjectBuilderService.Config> {

    /** {@code from} value selecting the subject DN of the client's TLS certificate. */
    public static final String CLIENT_TLS_SUBJECT = "clientTlsSubject";
    /** {@code from} value selecting the RFC 822 (email) subject alternative names of the client's TLS certificate. */
    public static final String CLIENT_TLS_SAN_RFC822_NAME = "clientTlsSanRfc822Name";
    /** {@code from} value selecting the directory name subject alternative names of the client's TLS certificate. */
    public static final String CLIENT_TLS_SAN_DIR_NAME = "clientTlsSanDirName";
    /** {@code from} value selecting the DNS subject alternative names of the client's TLS certificate. */
    public static final String CLIENT_TLS_SAN_DNS_NAME = "clientTlsSanDnsName";
    /** {@code from} value selecting the URI subject alternative names of the client's TLS certificate. */
    public static final String CLIENT_TLS_SAN_URI = "clientTlsSanUri";
    /** {@code from} value selecting the IP address subject alternative names of the client's TLS certificate. */
    public static final String CLIENT_TLS_SAN_IP_ADDRESS = "clientTlsSanIpAddress";
    /** {@code else} mapping value that passes the extracted name through unchanged. */
    public static final String ELSE_IDENTITY = "identity";
    /** {@code else} mapping value that discards the extracted name, contributing no principal. */
    public static final String ELSE_ANONYMOUS = "anonymous";

    /**
     * Constructor invoked by the plugin service loading machinery.
     */
    public DefaultTransportSubjectBuilderService() {
        // Intentionally empty
    }

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
    /**
     * Configuration for the {@link DefaultTransportSubjectBuilderService} plugin.
     *
     * @param addPrincipals the principal adder configurations, each describing where to extract names from,
     *        how to map them and which principal factory to use.
     */
    public record Config(List<PrincipalAdderConf> addPrincipals) {
        /**
         * Validates each principal adder configuration eagerly, rejecting invalid
         * {@code from}, {@code map} or {@code principalFactory} values.
         */
        public Config {
            for (PrincipalAdderConf adder : addPrincipals) {
                // call methods for validation side-effect
                buildExtractor(adder.from());
                MappingRule.buildMappingRules(adder.map());
                buildPrincipalFactory(adder.principalFactory());
            }
        }
    }

    @Nullable
    List<PrincipalAdder> adders;

    @Override
    public void initialize(@Nullable Config config) {
        adders = Plugins.requireConfig(this, config).addPrincipals().stream()
                .map(addConf -> new PrincipalAdder(buildExtractor(addConf.from()),
                        MappingRule.buildMappingRules(addConf.map()),
                        buildPrincipalFactory(addConf.principalFactory())))
                .toList();
    }

    static PrincipalFactory buildPrincipalFactory(String principalFactory) {
        return ServiceLoader.load(PrincipalFactory.class).stream()
                .filter(provider -> provider.type().getName().equals(principalFactory))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("`principalFactory` '%s' not found.".formatted(principalFactory)))
                .get();
    }

    @NonNull
    static Function<Object, Stream<String>> buildExtractor(String from) {
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
        // nothing to do
    }

}
