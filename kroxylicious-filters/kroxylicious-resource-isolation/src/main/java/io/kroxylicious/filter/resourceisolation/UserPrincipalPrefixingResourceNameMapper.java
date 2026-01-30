/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.resourceisolation;

import java.util.Objects;
import java.util.Optional;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

class UserPrincipalPrefixingResourceNameMapper implements ResourceNameMapper {

    private static final String SEPARATOR = "-";

    @Override
    public String map(MapperContext mapperContext, ResourceIsolation.ResourceType resourceType, String unmappedResourceName) {
        var user = getAuthenticatedPrincipal(mapperContext.authenticateSubject());
        return user.map(authId -> doMap(authId, unmappedResourceName))
                .orElse(unmappedResourceName);
    }

    private static String doMap(String authId, String unmappedResourceName) {
        return authId + SEPARATOR + unmappedResourceName;
    }

    @Override
    public String unmap(MapperContext mapperContext, ResourceIsolation.ResourceType resourceType, String mappedResourceName) {
        var user = getAuthenticatedPrincipal(mapperContext.authenticateSubject());
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
    public boolean isInNamespace(MapperContext mapperContext, ResourceIsolation.ResourceType resourceType, String mappedResourceName) {
        var user = getAuthenticatedPrincipal(mapperContext.authenticateSubject());
        return user.map(authId -> mappedResourceName.startsWith(authId + SEPARATOR))
                .orElse(false);
    }

    private static Optional<String> getAuthenticatedPrincipal(Subject authenticateSubject) {
        var authenticatedSubject = Objects.requireNonNull(authenticateSubject);
        return authenticatedSubject.uniquePrincipalOfType(User.class)
                .map(User::name);
    }

}
