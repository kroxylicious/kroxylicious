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
        LEARN_TOPIC_NAMES(new RawTaggedField(14343085, "kroxylicious.io/learn-topic-names".getBytes(StandardCharsets.UTF_8)));

        private final RawTaggedField rawTaggedField;

        Tag(RawTaggedField rawTaggedField) {
            this.rawTaggedField = rawTaggedField;
        }

        @VisibleForTesting
        RawTaggedField getRawTaggedField() {
            return rawTaggedField;
        }
    }

    public static void removeTags(RequestHeaderData header) {
        List<RawTaggedField> rawTaggedFields = header.unknownTaggedFields();
        for (Tag value : Tag.values()) {
            rawTaggedFields.remove(value.getRawTaggedField());
        }
    }

    public static void tag(RequestHeaderData headerData, Tag tag) {
        // clone the data, we don't want to hand out a reference that Filters can mutate
        headerData.unknownTaggedFields().add(new RawTaggedField(tag.rawTaggedField.tag(), tag.rawTaggedField.data().clone()));
    }

    public static boolean containsTag(RequestHeaderData headerData, Tag tag) {
        return headerData.unknownTaggedFields().contains(tag.rawTaggedField);
    }
}
