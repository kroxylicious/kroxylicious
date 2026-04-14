/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * Utility for tagging message headers with internal tags. Such tags should not be propagated
 * to the upstream broker.
 */
public class RequestHeaderTagger {
    public enum Tag {
        LEARN_TOPIC_NAMES(14343085, "kroxylicious.io/learn-topic-names");

        private final int tagId;
        private final String tagData;

        Tag(int tagId, String tagData) {
            this.tagId = tagId;
            this.tagData = tagData;
        }

        private RawTaggedField rawTaggedField() {
            return new RawTaggedField(tagId, tagData.getBytes(StandardCharsets.UTF_8));
        }

        private boolean matches(RawTaggedField rawTaggedField) {
            return rawTaggedField.tag() == tagId && tagData.equals(new String(rawTaggedField.data(), StandardCharsets.UTF_8));
        }

        @VisibleForTesting
        RawTaggedField getRawTaggedField() {
            return rawTaggedField();
        }
    }

    public static void removeTags(RequestHeaderData header) {
        List<RawTaggedField> rawTaggedFields = header.unknownTaggedFields();
        for (Tag value : Tag.values()) {
            rawTaggedFields.removeIf(value::matches);
        }
    }

    public static void tag(RequestHeaderData headerData, Tag tag) {
        headerData.unknownTaggedFields().add(tag.rawTaggedField());
    }

    public static boolean containsTag(RequestHeaderData headerData, Tag tag) {
        return headerData.unknownTaggedFields().stream().anyMatch(tag::matches);
    }
}
