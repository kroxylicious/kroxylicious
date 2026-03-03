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
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import edu.umd.cs.findbugs.annotations.Nullable;

public interface MetadataChecksumGenerator {
    Logger LOGGER = LoggerFactory.getLogger(MetadataChecksumGenerator.class);
    String CHECKSUM_CONTEXT_KEY = "kroxylicious.io/referent-checksum-generator";
    String NO_CHECKSUM_SPECIFIED = "";

    default void appendMetadata(HasMetadata entity) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("appendMetadata for: {}", ResourcesUtil.namespacedSlug(entity));
        }
        ObjectMeta objectMeta = entity.getMetadata();
        appendString(Objects.requireNonNull(objectMeta.getUid(), KubernetesResourceUtil.getName(objectMeta) + " is missing a UID"));
        appendVersionSpecifier(objectMeta);
        Optional<String> referentChecksum = Annotations.readReferentChecksumFrom(entity);
        referentChecksum.ifPresent(this::appendString);
    }

    void appendString(@Nullable String value);

    void appendLong(Long value);

    String encode();

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
