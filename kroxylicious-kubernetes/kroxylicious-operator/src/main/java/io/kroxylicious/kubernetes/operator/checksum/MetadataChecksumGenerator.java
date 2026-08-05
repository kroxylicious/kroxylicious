/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checksum;

import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.utils.KubernetesResourceUtil;

import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.OperatorLoggingKeys;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Generates checksums from Kubernetes resource metadata.
 */
public interface MetadataChecksumGenerator {
    /** Logger for this interface. */
    Logger LOGGER = LoggerFactory.getLogger(MetadataChecksumGenerator.class);
    /** Context key used to store and retrieve a checksum generator. */
    String CHECKSUM_CONTEXT_KEY = "kroxylicious.io/referent-checksum-generator";
    /** Sentinel value indicating no checksum has been computed. */
    String NO_CHECKSUM_SPECIFIED = "";

    /**
     * Appends the metadata of the given entity to the checksum.
     *
     * @param entity the Kubernetes resource whose metadata is appended
     */
    default void appendMetadata(HasMetadata entity) {
        LOGGER.atDebug()
                .addKeyValue(OperatorLoggingKeys.KIND, ResourcesUtil.kind(entity))
                .addKeyValue(OperatorLoggingKeys.NAME, ResourcesUtil.name(entity))
                .addKeyValue(OperatorLoggingKeys.NAMESPACE, ResourcesUtil.namespace(entity))
                .log("AppendMetadata for entity");
        ObjectMeta objectMeta = entity.getMetadata();
        appendString(Objects.requireNonNull(objectMeta.getUid(), KubernetesResourceUtil.getName(objectMeta) + " is missing a UID"));
        appendVersionSpecifier(objectMeta);
        Optional<String> referentChecksum = Annotations.readReferentChecksumFrom(entity);
        referentChecksum.ifPresent(this::appendString);
    }

    /**
     * Appends a string value to the checksum.
     *
     * @param value the string to append, or {@code null} to skip
     */
    void appendString(@Nullable String value);

    /**
     * Appends a long value to the checksum.
     *
     * @param value the long value to append
     */
    void appendLong(Long value);

    /**
     * Encodes the accumulated checksum as a string.
     *
     * @return the encoded checksum, or {@link #NO_CHECKSUM_SPECIFIED} if no data was appended
     */
    String encode();

    /**
     * Appends a version specifier from the given object metadata to the checksum.
     *
     * @param objectMeta the object metadata whose generation or resource version is appended
     */
    default void appendVersionSpecifier(ObjectMeta objectMeta) {
        Long generation = objectMeta.getGeneration();
        if (generation != null) {
            appendLong(generation);
        }
        else {
            // Some resources do not have a generation. For example, ConfigMap and Secret are self-contained
            // resources where the state is the resource. They do not have a status subresource or a need for
            // a generation field. Instead, we can include the resource version, which is modified with every
            // write to the resource.
            appendString(objectMeta.getResourceVersion());
        }
    }

}
