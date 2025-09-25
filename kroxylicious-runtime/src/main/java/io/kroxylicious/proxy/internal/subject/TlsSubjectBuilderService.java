/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.List;
import java.util.function.UnaryOperator;

import edu.umd.cs.findbugs.annotations.Nullable;

public class TlsSubjectBuilderService implements SubjectBuilderService<TlsSubjectBuilderService.Config> {

    private Config config;

    public record Config(
            @Nullable String subjectNameFormat,
            @Nullable List<String> subjectMappingRules,
            @Nullable List<String> sanRfc822NameMappingRules,
            @Nullable List<String> sanDnsNameMappingRules,
            @Nullable List<String> sanDirectoryNameMappingRules,
            @Nullable List<String> sanUriMappingRules,
            @Nullable List<String> sanIpAddressMappingRules
    ) { }

    @Override
    public void initialize(Config config) {
        this.config = config;
    }

    @Override
    public SubjectBuilder build() {
        // TODO need to distinguish the cases
        // 1. I want to use the default name format
        // 2. I don't want to include a principal for the subject principal
        return new TlsSubjectBuilder(
                config.subjectNameFormat(),
                buildRule(config.subjectMappingRules(), null),
                buildRule(config.sanRfc822NameMappingRules(), null),
                buildRule(config.sanDnsNameMappingRules(), null),
                buildRule(config.sanDirectoryNameMappingRules(), null),
                buildRule(config.sanUriMappingRules(), null),
                buildRule(config.sanIpAddressMappingRules(), null)
        );
    }

    private @Nullable UnaryOperator<String> buildRule(List<String> rules, @Nullable UnaryOperator<String> defaultRule) {
        if (rules == null) {
            return defaultRule;
        }
        if (rules.equals(List.of("DEFAULT"))) {
            return UnaryOperator.identity();
        }
        int def = rules.indexOf("DEFAULT");
        if (def != -1 && def != rules.size() - 1) {
            throw new IllegalArgumentException("DEFAULT rule must be the last rule in the list");
        }
        return new TlsSubjectMappingRules(rules.subList(0, def == -1 ? rules.size() : def));
    }
}
