/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.authentication.PrincipalFactory;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for a principal adder, which is responsible for contributing zero or more principals to the subject.
 * @param from Names a function for extracting a string value from a {@code Context}.
 * @param map An optional list of mappings to apply to the `from`-extracted string.
 * @param principalFactory The name of a {@link PrincipalFactory} implementation class.
 */
public record PrincipalAdderConf(@JsonProperty(required = true) String from,
                                 @Nullable List<Map> map,
                                 @JsonProperty(required = true) String principalFactory) {
    public PrincipalAdderConf {
        // call methods for validation side-effect
        DefaultSaslSubjectBuilderService.buildExtractor(from);
        DefaultSaslSubjectBuilderService.buildMappingRules(map);
        DefaultSaslSubjectBuilderService.buildPrincipalFactory(principalFactory);
    }
}
