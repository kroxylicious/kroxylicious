/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.PrincipalFactory;

public record PrincipalAdder(
                             Function<Object, Stream<String>> extractor,
                             List<MappingRule> rules,
                             PrincipalFactory<?> factory) {
    Stream<Principal> createPrincipals(Object context) {
        return extractor.apply(context)
                .flatMap(extractedName -> rules.stream().map(rule -> rule.apply(extractedName))
                        .filter(Optional::isPresent)
                        .findFirst()
                        .stream())
                .flatMap(Optional::stream)
                .map(factory::newPrincipal);
    }
}
