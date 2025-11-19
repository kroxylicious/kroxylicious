/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.proxy.authentication.PrincipalFactory;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilderService;
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

    }
    
    List<PrincipalAdder> adders;

    @Override
    public void initialize(Config config) {
        adders = Plugins.requireConfig(this, config).addPrincipals().stream()
                .map(addConf -> new PrincipalAdder(buildExtractor(addConf.from()),
                        buildMappingRules(addConf.map()),
                        buildPrincipalFactory(addConf.principalFactory())))
                .toList();
    }

    private static PrincipalFactory<?> buildPrincipalFactory(String principalFactory) {
        return ServiceLoader.load(PrincipalFactory.class).stream()
                .filter(provider -> provider.type().getName().equals(principalFactory))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("`principalFactory` '%s' not found.".formatted(principalFactory)))
                .get();
    }

    @NonNull
    private static List<MappingRule> buildMappingRules(@Nullable List<Map> maps) {
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
        if (numElses > 1) {
            throw new IllegalArgumentException("An `else` mapping may only occur at most once, as the last element of `map`.");
        }
        else if (firstElseIndex != -1 && firstElseIndex < maps.size() - 1) {
            throw new IllegalArgumentException("An `else` mapping may only occur as the last element of `map`.");
        }
        return maps.stream().map(DefaultSaslSubjectBuilderService::buildMappingRule).toList();
    }

    @NonNull
    private static MappingRule buildMappingRule(Map map) {
        if (map.replaceMatch() != null) {
            return new ReplaceMatchMappingRule(map.replaceMatch());
        }
        else if (ELSE_IDENTITY.equals(map.else_())) {
            return new IdentityMappingRule();
        }
        else if (ELSE_ANONYMOUS.equals(map.else_())) {
            return s -> Optional.empty();
        }
        else {
            throw new IllegalArgumentException("Unknown `else` map '%s', supported values are: '%s', '%s'."
                    .formatted(map.else_(), ELSE_IDENTITY, ELSE_ANONYMOUS));
        }
    }

    @NonNull
    private static Function<Object, Stream<String>> buildExtractor(String from) {
        return switch (from) {
            case SASL_AUTHORIZED_ID -> context -> Stream.of(((SaslSubjectBuilder.Context) context).clientSaslContext().authorizationId());
            default -> throw new IllegalArgumentException("Unknown `from` '%s', supported values are: %s."
                    .formatted(from,
                            Stream.of(SASL_AUTHORIZED_ID).map(s -> '\'' + s + '\'')
                                    .collect(Collectors.joining(", "))));
        };
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
