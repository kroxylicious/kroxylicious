/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.CRC32;

import io.fabric8.kubernetes.api.model.HasMetadata;

public class MetadataChecksumGenerator {

    public static final String REFERENT_CHECKSUM_ANNOTATION = "kroxylicious.io/referent-checksum";
    public static final String NO_CHECKSUM_SPECIFIED = "";

    private MetadataChecksumGenerator() {
    }

    public static String checksumFor(HasMetadata... metadataSources) {
        var checksum = new CRC32();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        for (HasMetadata metadataSource : metadataSources) {
            var objectMeta = metadataSource.getMetadata();
            checksum.update(objectMeta.getUid().getBytes(StandardCharsets.UTF_8));
            if (objectMeta.getGeneration() != null) {
                byteBuffer.putLong(0, objectMeta.getGeneration());
                checksum.update(byteBuffer);
            }
            else {
                // Some resources do not have a generation. For example, ConfigMap and Secret are self-contained
                // resources where the state is the resource. They do not have a status subresource or a need for
                // a generation field. Instead, we can include the resource version, which is modified with every
                // write to the resource.
                checksum.update(objectMeta.getResourceVersion().getBytes(StandardCharsets.UTF_8));
            }
        }
        byteBuffer.putLong(0, checksum.getValue());
        return Base64.getEncoder().withoutPadding().encodeToString(byteBuffer.array());
    }
}
