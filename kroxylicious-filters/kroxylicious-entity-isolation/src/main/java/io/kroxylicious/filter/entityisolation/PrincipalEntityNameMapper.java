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
import io.kroxylicious.proxy.authentication.User;

class PrincipalEntityNameMapper implements EntityNameMapper {

    private static final String SEPARATOR = "-";

    private Class<? extends Principal> uniquePrincipalType;

    PrincipalEntityNameMapper(Class<? extends Principal> uniquePrincipalType) {
        this.uniquePrincipalType = Objects.requireNonNull(uniquePrincipalType);
        if (!uniquePrincipalType.isAnnotationPresent(Unique.class)) {
            throw new IllegalArgumentException(uniquePrincipalType.getName() + " is not a unique principal type");
        }
    }

    @Override
    public String map(MapperContext mapperContext, EntityIsolation.ResourceType resourceType, String unmappedResourceName) {
        uniquePrincipalType = User.class;
        var user = getAuthenticatedPrincipal(mapperContext.authenticateSubject(), uniquePrincipalType);
        return user.map(authId -> doMap(authId, unmappedResourceName))
                .orElse(unmappedResourceName);
    }

    private static String doMap(String authId, String unmappedResourceName) {
        return authId + SEPARATOR + unmappedResourceName;
    }

    @Override
    public String unmap(MapperContext mapperContext, EntityIsolation.ResourceType resourceType, String mappedResourceName) {
        var user = getAuthenticatedPrincipal(mapperContext.authenticateSubject(), User.class);
        return user.map(authId -> doUnmap(authId, mappedResourceName))
                .orElse(mappedResourceName);
    }

    private String doUnmap(String authId, String mappedResourceName) {
        var prefix = authId + SEPARATOR;
        if (mappedResourceName.startsWith(prefix)) {
            return mappedResourceName.substring(prefix.length());
        }
        else {
            throw new IllegalArgumentException("Resource name ''%s' does not belong to the namespace belonging to '%s'".formatted(mappedResourceName, authId));
        }
    }

    @Override
    public boolean isInNamespace(MapperContext mapperContext, EntityIsolation.ResourceType resourceType, String mappedResourceName) {
        var user = getAuthenticatedPrincipal(mapperContext.authenticateSubject(), User.class);
        return user.map(authId -> mappedResourceName.startsWith(authId + SEPARATOR))
                .orElse(false);
    }

    private static Optional<String> getAuthenticatedPrincipal(Subject authenticateSubject, Class<? extends Principal> uniquePrincipalType) {
        var authenticatedSubject = Objects.requireNonNull(authenticateSubject);
        return authenticatedSubject.uniquePrincipalOfType(uniquePrincipalType)
                .map(Principal::name);
    }

}
