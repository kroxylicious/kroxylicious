/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.proxy.authentication.PrincipalFactory;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilderService;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = DefaultSaslSubjectBuilderService.Config.class)
public class DefaultSaslSubjectBuilderService implements SaslSubjectBuilderService<DefaultSaslSubjectBuilderService.Config> {

    public static final String SASL_AUTHORIZED_ID = "saslAuthorizedId";
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
    public void initialize(Config config) {
        adders = Plugins.requireConfig(this, config).addPrincipals().stream()
                .map(addConf -> new PrincipalAdder(buildExtractor(addConf.from()),
                        MappingRule.buildMappingRules(addConf.map()),
                        buildPrincipalFactory(addConf.principalFactory())))
                .toList();
    }

    static PrincipalFactory<?> buildPrincipalFactory(String principalFactory) {
        return ServiceLoader.load(PrincipalFactory.class).stream()
                .filter(provider -> provider.type().getName().equals(principalFactory))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("`principalFactory` '%s' not found.".formatted(principalFactory)))
                .get();
    }

    @NonNull
    static Function<Object, Stream<String>> buildExtractor(String from) {
        return switch (from) {
            case SASL_AUTHORIZED_ID -> context -> Stream.of(((SaslSubjectBuilder.Context) context).clientSaslContext().authorizationId());
            default -> throw new IllegalArgumentException("Unknown `from` '%s', supported values are: %s."
                    .formatted(from,
                            Stream.of(SASL_AUTHORIZED_ID).map(s -> '\'' + s + '\'')
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
    public SaslSubjectBuilder build() {
        return new DefaultSubjectBuilder(Objects.requireNonNull(adders, "build() called before initialize()"));
    }

    @Override
    public void close() {
        // We have no closeable resources
    }

}
