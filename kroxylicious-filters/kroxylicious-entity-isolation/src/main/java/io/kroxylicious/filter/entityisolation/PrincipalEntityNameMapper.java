/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Objects;
import java.util.Optional;

import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.Unique;

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
    public String map(MapperContext mapperContext, EntityIsolation.EntityType resourceType, String downstreamResourceName) {
        var user = getValidatedValidatedPrincipalName(mapperContext.authenticatedSubject());
        return user.map(authId -> doMap(authId, downstreamResourceName))
                .orElse(downstreamResourceName);
    }

    private String doMap(String authId, String unmappedResourceName) {
        return authId + separator + unmappedResourceName;
    }

    @Override
    public String unmap(MapperContext mapperContext, EntityIsolation.EntityType resourceType, String upstreamResourceName) {
        var user = getValidatedValidatedPrincipalName(mapperContext.authenticatedSubject());
        return user.map(authId -> doUnmap(authId, upstreamResourceName))
                .orElse(upstreamResourceName);
    }

    private String doUnmap(String authId, String mappedResourceName) {
        var prefix = authId + separator;
        if (mappedResourceName.startsWith(prefix)) {
            return mappedResourceName.substring(prefix.length());
        }
        else {
            throw new IllegalArgumentException("Resource name '%s' does not belong to the namespace belonging to '%s'".formatted(mappedResourceName, authId));
        }
    }

    @Override
    public boolean isInNamespace(MapperContext mapperContext, EntityIsolation.EntityType resourceType, String upstreamResourceName) {
        var user = getValidatedValidatedPrincipalName(mapperContext.authenticatedSubject());
        return user.map(authId -> upstreamResourceName.startsWith(authId + separator))
                .orElse(false);
    }

    private Optional<String> getValidatedValidatedPrincipalName(Subject authenticateSubject) {
        var authenticatedSubject = Objects.requireNonNull(authenticateSubject);
        var name = authenticatedSubject.uniquePrincipalOfType(uniquePrincipalType).map(Principal::name);
        name.ifPresent(n -> {
            if (n.contains(separator)) {
                throw new UnacceptableEntityNameException("Principal name '%s' is unaccepted as it contains the separator '%s'".formatted(n, separator));
            }
        });
        return name;
    }

}
