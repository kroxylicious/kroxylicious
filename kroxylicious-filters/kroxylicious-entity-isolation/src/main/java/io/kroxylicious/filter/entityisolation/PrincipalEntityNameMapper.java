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
import io.kroxylicious.proxy.authentication.Subject;
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
 * If the channel does not have an authenticated subject, no mapping is performed.
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
    public void validateContext(MapperContext mapperContext) throws EntityMapperException {
        Objects.requireNonNull(mapperContext);
        var principal = getPrincipalName(mapperContext)
                .orElseThrow(() -> new EntityMapperException(
                        "The PrincipalEntityNameMapper requires an authenticated subject with a unique principal of type %s with a non-empty name, got subject %s"
                                .formatted(
                                        uniquePrincipalType.getSimpleName(), mapperContext.authenticatedSubject())));

        if (principal.name().contains(separator)) {
            throw new EntityMapperException("Principal '%s' is unacceptable as it contains the mapping separator '%s'".formatted(principal, separator));
        }
    }

    @Override
    public String map(MapperContext mapperContext, EntityType entityType, String downstreamResourceName) {
        Objects.requireNonNull(mapperContext);
        Objects.requireNonNull(entityType);
        Objects.requireNonNull(downstreamResourceName);

        return getPrincipalName(mapperContext)
                .map(Principal::name)
                .map(name -> doMap(name, downstreamResourceName))
                .orElseThrow(() -> new IllegalStateException("Unexpected exception mapping entity name '%s' for %s".formatted(downstreamResourceName, mapperContext)));
    }

    private String doMap(String principal, String downstreamResourceName) {
        return principal + separator + downstreamResourceName;
    }

    @Override
    public String unmap(MapperContext mapperContext, EntityType entityType, String upstreamResourceName) {
        Objects.requireNonNull(mapperContext);
        Objects.requireNonNull(entityType);
        Objects.requireNonNull(upstreamResourceName);
        return getPrincipalName(mapperContext)
                .map(Principal::name)
                .map(name -> doUnmap(name, upstreamResourceName))
                .orElseThrow(() -> new IllegalStateException("Unexpected exception unmapping entity name '%s' for %s".formatted(upstreamResourceName, mapperContext)));
    }

    @Nullable
    private String doUnmap(String authId, String mappedResourceName) {
        var prefix = authId + separator;
        if (mappedResourceName.startsWith(prefix)) {
            return mappedResourceName.substring(prefix.length());
        }
        return null;
    }

    @Override
    public boolean isOwnedByContext(MapperContext mapperContext, EntityType entityType, String upstreamResourceName) {
        Objects.requireNonNull(mapperContext);
        Objects.requireNonNull(entityType);
        Objects.requireNonNull(upstreamResourceName);
        return getPrincipalName(mapperContext)
                .map(Principal::name)
                .map(name -> doUnmap(name, upstreamResourceName) != null)
                .orElse(false);
    }

    private Optional<String> getValidatedValidatedPrincipalName(Subject authenticateSubject) {
        var authenticatedSubject = Objects.requireNonNull(authenticateSubject);
        var name = authenticatedSubject.uniquePrincipalOfType(uniquePrincipalType).map(Principal::name);
        name.ifPresent(n -> {
            if (n.contains(separator)) {
                throw new EntityMapperException("Principal name '%s' is unaccepted as it contains the separator '%s'".formatted(n, separator));
            }
        });
        return name;
    }

    private Optional<? extends Principal> getPrincipalName(MapperContext mapperContext) {
        return Optional.of(mapperContext)
                .map(MapperContext::authenticatedSubject)
                .flatMap(a -> a.uniquePrincipalOfType(uniquePrincipalType))
                .filter(Predicate.not(p -> p.name() == null || p.name().isBlank()));
    }
}
