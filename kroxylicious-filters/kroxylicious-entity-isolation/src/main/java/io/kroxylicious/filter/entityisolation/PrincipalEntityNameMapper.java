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
    public String map(MapperContext mapperContext, EntityIsolation.ResourceType resourceType, String unmappedResourceName) {
        var user = getValidatedName(mapperContext.authenticatedSubject());
        return user.map(authId -> doMap(authId, unmappedResourceName))
                .orElse(unmappedResourceName);
    }

    private String doMap(String authId, String unmappedResourceName) {
        return authId + separator + unmappedResourceName;
    }

    @Override
    public String unmap(MapperContext mapperContext, EntityIsolation.ResourceType resourceType, String mappedResourceName) {
        var user = getValidatedName(mapperContext.authenticatedSubject());
        return user.map(authId -> doUnmap(authId, mappedResourceName))
                .orElse(mappedResourceName);
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
    public boolean isInNamespace(MapperContext mapperContext, EntityIsolation.ResourceType resourceType, String mappedResourceName) {
        var user = getValidatedName(mapperContext.authenticatedSubject());
        return user.map(authId -> mappedResourceName.startsWith(authId + separator))
                .orElse(false);
    }

    private Optional<String> getValidatedName(Subject authenticateSubject) {
        var authenticatedSubject = Objects.requireNonNull(authenticateSubject);
        var name = authenticatedSubject.uniquePrincipalOfType(uniquePrincipalType).map(Principal::name);
        name.ifPresent(n -> {
            if (n.contains(separator)) {
                throw new UnacceptableEntityNameException("Principal name '%s' may not contain the separator '%s'".formatted(n, separator));
            }
        });
        return name;
    }

}
