/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checksum;

import java.util.Map;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

public interface MetadataChecksumGenerator {

    String REFERENT_CHECKSUM_ANNOTATION = "kroxylicious.io/referent-checksum";
    String CHECKSUM_CONTEXT_KEY = "kroxylicious.io/referent-checksum-generator";
    String NO_CHECKSUM_SPECIFIED = "";

    default void appendMetadata(ObjectMeta objectMeta) {
        appendString(objectMeta.getUid());
        appendVersionSpecifier(objectMeta);
        Map<String, String> annotations = objectMeta.getAnnotations();
        if (annotations != null && annotations.containsKey(REFERENT_CHECKSUM_ANNOTATION)) {
            appendString(annotations.get(REFERENT_CHECKSUM_ANNOTATION));
        }
    }

    default void appendMetadata(HasMetadata entity) {
        appendMetadata(entity.getMetadata());
    }

    void appendString(String value);

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
