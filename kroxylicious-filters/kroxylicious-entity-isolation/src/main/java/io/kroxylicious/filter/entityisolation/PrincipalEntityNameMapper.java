/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import io.kroxylicious.filter.entityisolation.EntityIsolation.EntityType;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Unique;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An entity name mapper that forms the isolation using the principal name.
 * <br/>
 * When mapping from the downstream to the upstream, resource names are prepended
 * with the principal name of the authenticated subject and a separator.
 * <br/>
 * When mapping from the upstream to the downstream, the principal prefix and separator
 * are removed.
 * <br/>
 * It is an error if a channel does not have an authenticated subject
 */
class PrincipalEntityNameMapper implements EntityNameMapper {
    private final Class<? extends Principal> uniquePrincipalType;
    private final String separator;

    PrincipalEntityNameMapper(Class<? extends Principal> uniquePrincipalType, String separator) {
        this.uniquePrincipalType = Objects.requireNonNull(uniquePrincipalType);
        this.separator = Objects.requireNonNull(separator);
        if (!uniquePrincipalType.isAnnotationPresent(Unique.class)) {
            throw new IllegalArgumentException(uniquePrincipalType.getName() + " is not a unique principal type.");
        }
        if (separator.isEmpty()) {
            throw new IllegalArgumentException(separator + " is an unacceptable separator.");
        }
    }

    @Override
    public String map(MapperContext mapperContext, EntityType entityType, String downstreamResourceName) {
        Objects.requireNonNull(mapperContext);
        Objects.requireNonNull(entityType);
        Objects.requireNonNull(downstreamResourceName);

        var validatedPrincipal = getValidatedPrincipal(mapperContext);
        return doMap(validatedPrincipal.name(), downstreamResourceName);
    }

    private String doMap(String principal, String downstreamResourceName) {
        // Once we start mapping topic names, we must verify the length upstream name doesn't violate the topic naming org.apache.kafka.common.internals.Topic.isValid
        // Also if https://cwiki.apache.org/confluence/display/KAFKA/KIP-1233%3A+Maximum+lengths+for+resource+names+and+IDs is accepted there may be rules to apply to groupIds/transactionalIds
        return buildPrefix(principal) + downstreamResourceName;
    }

    @Override
    public boolean isOwnedByContext(MapperContext mapperContext, EntityType entityType, String upstreamResourceName) {
        Objects.requireNonNull(mapperContext);
        Objects.requireNonNull(entityType);
        Objects.requireNonNull(upstreamResourceName);
        return Optional.of(getValidatedPrincipal(mapperContext))
                .map(Principal::name)
                .map(name -> doUnmap(name, upstreamResourceName) != null)
                .orElse(false);
    }

    @Override
    public String unmap(MapperContext mapperContext, EntityType entityType, String upstreamResourceName) {
        Objects.requireNonNull(mapperContext);
        Objects.requireNonNull(entityType);
        Objects.requireNonNull(upstreamResourceName);

        var validatedPrincipal = getValidatedPrincipal(mapperContext);
        return Optional.of(validatedPrincipal)
                .map(Principal::name)
                .map(name -> doUnmap(name, upstreamResourceName))
                .orElseThrow(() -> new IllegalStateException("Unexpected exception unmapping entity name '%s' for %s".formatted(upstreamResourceName, mapperContext)));
    }

    @Nullable
    private String doUnmap(String principalName, String mappedResourceName) {
        var prefix = buildPrefix(principalName);
        if (mappedResourceName.startsWith(prefix)) {
            return mappedResourceName.substring(prefix.length());
        }
        return null;
    }

    private String buildPrefix(String principalName) {
        return principalName + separator;
    }

    private Principal getValidatedPrincipal(MapperContext mapperContext) {
        var principalOpt = Optional.of(mapperContext)
                .map(MapperContext::authenticatedSubject)
                .flatMap(a -> a.uniquePrincipalOfType(uniquePrincipalType));

        principalOpt.orElseThrow(() -> new EntityMapperException(
                "The PrincipalEntityNameMapper requires an authenticated subject with a unique principal of type %s with a non-empty name, got subject %s"
                        .formatted(
                                uniquePrincipalType.getSimpleName(), mapperContext.authenticatedSubject())));

        principalOpt.map(Principal::name)
                .filter(Predicate.not(name -> name.contains(separator)))
                .orElseThrow(() -> new EntityMapperException(
                        "Principal '%s' is unacceptable as it contains the mapping separator '%s'".formatted(principalOpt.get(), separator)));
        return principalOpt.get();
    }
}
