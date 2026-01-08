/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RequestHeaderTaggerTest {

    @Test
    void testTag() {
        // given
        RequestHeaderData headerData = new RequestHeaderData();
        // when
        RequestHeaderTagger.tag(headerData, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        // then
        RawTaggedField expected = new RawTaggedField(14343085, "kroxylicious.io/learn-topic-names".getBytes(StandardCharsets.UTF_8));
        assertThat(headerData.unknownTaggedFields()).contains(expected);
    }

    @Test
    void noTagsOnHeader() {
        // given
        RequestHeaderData headerData = new RequestHeaderData();
        // when
        boolean actual = RequestHeaderTagger.containsTag(headerData, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        // then
        assertThat(actual).isFalse();
    }

    @Test
    void tagOnHeader() {
        // given
        RequestHeaderData headerData = new RequestHeaderData();
        RequestHeaderTagger.tag(headerData, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        // when
        boolean actual = RequestHeaderTagger.containsTag(headerData, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        // then
        assertThat(actual).isTrue();
    }

    // this is to check that if a Filter mutates the tag data, the tag no longer matches.
    @Test
    void mutatingTagDataHasNoSideEffects() {
        // given
        byte[] originalData = RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES.getRawTaggedField().data().clone();
        RequestHeaderData headerData = new RequestHeaderData();
        RequestHeaderTagger.tag(headerData, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        headerData.unknownTaggedFields().iterator().next().data()[0] = 5;
        // when
        boolean actual = RequestHeaderTagger.containsTag(headerData, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        // then
        assertThat(actual).isFalse();
        byte[] currentData = RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES.getRawTaggedField().data().clone();
        assertThat(currentData).isEqualTo(originalData);
    }

    @Test
    void knownTagButUnknownData() {
        // given
        RequestHeaderData headerData = new RequestHeaderData();
        headerData.unknownTaggedFields().add(new RawTaggedField(14343085, new byte[0]));
        // when
        boolean actual = RequestHeaderTagger.containsTag(headerData, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        // then
        assertThat(actual).isFalse();
    }

    @Test
    void knownDataButUnknownTag() {
        // given
        RequestHeaderData headerData = new RequestHeaderData();
        headerData.unknownTaggedFields().add(new RawTaggedField(1, "kroxylicious.io/learn-topic-names".getBytes(StandardCharsets.UTF_8)));
        // when
        boolean actual = RequestHeaderTagger.containsTag(headerData, RequestHeaderTagger.Tag.LEARN_TOPIC_NAMES);
        // then
        assertThat(actual).isFalse();
    }
}