/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checksum;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

public interface MetadataChecksumGenerator {

    String REFERENT_CHECKSUM_ANNOTATION = "kroxylicious.io/referent-checksum";
    String CHECKSUM_CONTEXT_KEY = "kroxylicious.io/referent-checksum-generator";
    String NO_CHECKSUM_SPECIFIED = "";

    static String checksumFor(HasMetadata... metadataSources) {
        var checksum = new Crc32ChecksumGenerator();

        for (HasMetadata metadataSource : metadataSources) {
            var objectMeta = metadataSource.getMetadata();
            checksum.appendMetadata(objectMeta);
        }
        return checksum.encode();
    }

    void appendMetadata(ObjectMeta objectMeta);

    void appendMetadata(HasMetadata entity);

    void appendString(String value);

    void appendLong(Long value);

    String encode();
}
