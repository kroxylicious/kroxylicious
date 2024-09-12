/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import java.util.stream.Stream;

import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import static java.nio.charset.StandardCharsets.UTF_8;

public class UnknownTaggedFields {

    public static Stream<String> unknownTaggedFieldsToStrings(Message message, int tag) {
        return message.unknownTaggedFields()
                      .stream()
                      .filter(t -> t.tag() == tag)
                      .map(RawTaggedField::data)
                      .map(b -> new String(b, UTF_8));
    }
}
