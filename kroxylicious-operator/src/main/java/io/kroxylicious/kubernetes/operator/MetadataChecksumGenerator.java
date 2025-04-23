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

import javax.annotation.concurrent.NotThreadSafe;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

@NotThreadSafe
public class MetadataChecksumGenerator {

    public static final String REFERENT_CHECKSUM_ANNOTATION = "kroxylicious.io/referent-checksum";
    public static final String NO_CHECKSUM_SPECIFIED = "";
    private final CRC32 checksum;
    private final ByteBuffer byteBuffer;

    public MetadataChecksumGenerator() {
        checksum = new CRC32();
        byteBuffer = ByteBuffer.wrap(new byte[Long.BYTES]);
    }

    public static String checksumFor(HasMetadata... metadataSources) {
        var checksum = new MetadataChecksumGenerator();

        for (HasMetadata metadataSource : metadataSources) {
            var objectMeta = metadataSource.getMetadata();
            checksum.appendMetadata(objectMeta);
        }
        return checksum.encode();
    }

    public void appendMetadata(ObjectMeta objectMeta) {
        appendString(objectMeta.getUid());
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

    public void appendMetadata(HasMetadata metadataSource) {
        appendMetadata(metadataSource.getMetadata());
    }

    public void appendString(String value) {
        checksum.update(value.getBytes(StandardCharsets.UTF_8));
    }

    public void appendLong(Long value) {
        byteBuffer.putLong(0, value);
        checksum.update(byteBuffer);
    }

    public String encode() {
        byteBuffer.putLong(0, checksum.getValue());
        return Base64.getEncoder().withoutPadding().encodeToString(byteBuffer.array());
    }
}
