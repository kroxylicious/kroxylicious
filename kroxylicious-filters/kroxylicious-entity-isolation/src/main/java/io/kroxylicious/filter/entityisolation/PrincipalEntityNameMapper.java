/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import io.kroxylicious.filter.entityisolation.EntityIsolation.EntityType;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Unique;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An entity name mapper that forms the isolation using the principal name.
 * <br/>
 * When mapping from the downstream to the upstream, entity names are prepended
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

    /**
     * Regular expression for what Kafka considers legal for topic and group id names.
     * Kafka doesn't restrict transactionalId names,
     */
    static final Pattern KAFKA_RESOURCE_NAME_LEGAL_CHARS = Pattern.compile("[a-zA-Z0-9._\\-]+");

    /** Maximum length of a Kafka topic or groupId */
    static final int MAX_NAME_LENGTH = 249;

    PrincipalEntityNameMapper(Class<? extends Principal> uniquePrincipalType, String separator) {
        this.uniquePrincipalType = Objects.requireNonNull(uniquePrincipalType);
        this.separator = Objects.requireNonNull(separator);
        if (!uniquePrincipalType.isAnnotationPresent(Unique.class)) {
            throw new IllegalArgumentException(uniquePrincipalType.getName() + " is not a unique principal type.");
        }
        if (separator.isEmpty() || isIllegalKafkaName(separator)) {
            throw new IllegalArgumentException("'%s' is an unacceptable separator.".formatted(separator));
        }
    }

    @Override
    public String map(MapperContext mapperContext, EntityType entityType, String downstreamEntityName) {
        Objects.requireNonNull(mapperContext);
        Objects.requireNonNull(entityType);
        Objects.requireNonNull(downstreamEntityName);

        var validatedPrincipal = getValidatedPrincipal(mapperContext);
        return doMap(validatedPrincipal, downstreamEntityName, entityType);
    }

    private String doMap(Principal principal, String downstreamEntityName, EntityType entityType) {
        // Note that there is a KIP proposed that will affect https://cwiki.apache.org/confluence/display/KAFKA/KIP-1233%3A+Maximum+lengths+for+resource+names+and+IDs resource name validations
        var upstreamName = buildPrefix(principal.name()) + downstreamEntityName;
        if (isIllegalKafkaName(upstreamName, entityType)) {
            var pretty = entityType.name().toLowerCase(Locale.ROOT).replace("_", " ");
            throw new EntityMapperException(String.format("Upstream name generated for principal '%s' for resource '%s' ('%s') is not a valid kafka %s.",
                    downstreamEntityName, principal, upstreamName, pretty));
        }
        return upstreamName;
    }

    @Override
    public boolean isOwnedByContext(MapperContext mapperContext, EntityType entityType, String upstreamEntityName) {
        Objects.requireNonNull(mapperContext);
        Objects.requireNonNull(entityType);
        Objects.requireNonNull(upstreamEntityName);
        return Optional.of(getValidatedPrincipal(mapperContext))
                .map(Principal::name)
                .map(name -> doUnmap(name, upstreamEntityName) != null)
                .orElse(false);
    }

    @Override
    public String unmap(MapperContext mapperContext, EntityType entityType, String upstreamEntityName) {
        Objects.requireNonNull(mapperContext);
        Objects.requireNonNull(entityType);
        Objects.requireNonNull(upstreamEntityName);

        var validatedPrincipal = getValidatedPrincipal(mapperContext);
        return Optional.of(validatedPrincipal)
                .map(Principal::name)
                .map(name -> doUnmap(name, upstreamEntityName))
                .orElseThrow(() -> new IllegalStateException("Unexpected exception unmapping entity name '%s' for %s".formatted(upstreamEntityName, mapperContext)));
    }

    @Nullable
    private String doUnmap(String principalName, String mappedEntityName) {
        var prefix = buildPrefix(principalName);
        if (mappedEntityName.startsWith(prefix)) {
            return mappedEntityName.substring(prefix.length());
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

        principalOpt.map(Principal::name).ifPresent(name -> {
            if (name.contains(separator)) {
                throw new EntityMapperException(
                        "Principal '%s' is unacceptable as it contains the mapping separator '%s'".formatted(principalOpt.get(), separator));
            }
            if (isIllegalKafkaName(name)) {
                throw new EntityMapperException(
                        "Principal '%s' is unacceptable as it contains characters outside ASCII alphanumerics, '.', '_' and '-'".formatted(principalOpt.get()));
            }
        });

        return principalOpt.orElseThrow();
    }

    private static boolean isIllegalKafkaName(String separator) {
        return !KAFKA_RESOURCE_NAME_LEGAL_CHARS.matcher(separator).matches();
    }

    private static boolean isIllegalKafkaName(String upstreamName, EntityType entityType) {
        return switch (entityType) {
            case TOPIC_NAME, GROUP_ID -> isIllegalKafkaName(upstreamName) || upstreamName.length() > MAX_NAME_LENGTH;
            case TRANSACTIONAL_ID -> false;
        };
    }

}
