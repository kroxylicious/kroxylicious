/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.types.RawTaggedField;

import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * Utility for tagging message headers with internal tags. Such tags should not be propagated
 * to the upstream broker.
 */
public class RequestHeaderTagger {
    private static final RawTaggedField LEARN_TOPIC_NAMES_TAGGED_FIELD = new RawTaggedField(14343085,
            "kroxylicious.io/learn-topic-names".getBytes(StandardCharsets.UTF_8));

    /**
     * Not instantiable: all members are static.
     */
    private RequestHeaderTagger() {
        // Prevent construction
    }

    /**
     * The internal tags that may be applied to a request header.
     */
    public enum Tag {
        /** Marks a request as one from which the proxy should learn topic id to name mappings. */
        LEARN_TOPIC_NAMES;

        @VisibleForTesting
        RawTaggedField getRawTaggedField() {
            return copyOf(rawTaggedField(this));
        }
    }

    /**
     * Removes all internal tags from the given request header.
     *
     * @param header the request header to strip of internal tags
     */
    public static void removeTags(RequestHeaderData header) {
        List<RawTaggedField> rawTaggedFields = header.unknownTaggedFields();
        for (Tag value : Tag.values()) {
            RawTaggedField taggedField = rawTaggedField(value);
            rawTaggedFields.removeIf(rawTaggedField -> matches(rawTaggedField, taggedField));
        }
    }

    /**
     * Applies the given internal tag to the request header.
     *
     * @param headerData the request header to tag
     * @param tag the tag to apply
     */
    public static void tag(RequestHeaderData headerData, Tag tag) {
        headerData.unknownTaggedFields().add(copyOf(rawTaggedField(tag)));
    }

    /**
     * Determines whether the request header carries the given internal tag.
     *
     * @param headerData the request header to inspect
     * @param tag the tag to look for
     * @return true if the header contains the tag
     */
    public static boolean containsTag(RequestHeaderData headerData, Tag tag) {
        RawTaggedField taggedField = rawTaggedField(tag);
        return headerData.unknownTaggedFields().stream().anyMatch(rawTaggedField -> matches(rawTaggedField, taggedField));
    }

    private static boolean matches(RawTaggedField actual, RawTaggedField expected) {
        return actual.tag() == expected.tag() && Arrays.equals(actual.data(), expected.data());
    }

    private static RawTaggedField rawTaggedField(Tag tag) {
        return switch (tag) {
            case LEARN_TOPIC_NAMES -> LEARN_TOPIC_NAMES_TAGGED_FIELD;
        };
    }

    private static RawTaggedField copyOf(RawTaggedField rawTaggedField) {
        return new RawTaggedField(rawTaggedField.tag(), rawTaggedField.data().clone());
    }
}
