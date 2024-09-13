/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.requestresponsetestdef;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.reflect.ClassPath.ResourceInfo;

public record RequestResponseTestDef(
        String testName,
        ApiMessageType apiKey,
        RequestHeaderData header,
        ApiMessageTestDef request,
        ApiMessageTestDef response
) {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    public static Stream<RequestResponseTestDef> requestResponseTestDefinitions(List<ResourceInfo> resources) {
        return resources.stream()
                        .map(RequestResponseTestDef::readTestResource)
                        .flatMap(Arrays::stream)
                        .filter(Predicate.not(td -> td.get("disabled").asBoolean(false)))
                        .map(RequestResponseTestDef::buildRequestResponseTestDef);
    }

    private static JsonNode[] readTestResource(ResourceInfo resourceInfo) {
        try {
            var sourceFile = Path.of(resourceInfo.url().getPath()).getFileName().toString();
            var jsonNodes = MAPPER.reader().readValue(resourceInfo.url(), JsonNode[].class);
            for (int i = 0; i < jsonNodes.length; i++) {
                if (jsonNodes[i] instanceof ObjectNode objectNode) {
                    objectNode.put("source", String.format("%s[%d]", sourceFile, i));
                }
            }
            return jsonNodes;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static RequestResponseTestDef buildRequestResponseTestDef(JsonNode jsonNode) {
        var sourceNode = jsonNode.get("source");
        var descriptionNode = jsonNode.get("description");
        var apiMessageTypeNode = jsonNode.get("apiMessageType");
        var versionNode = jsonNode.get("version");
        Preconditions.checkNotNull(sourceNode, "source missing from test definition");
        var source = sourceNode.textValue();
        Preconditions.checkNotNull(apiMessageTypeNode, "apiMessageType missing from test definition: %s", source);
        Preconditions.checkNotNull(versionNode, "version missing from test definition: %s", source);
        var messageType = ApiMessageType.valueOf(apiMessageTypeNode.asText());
        var header = new RequestHeaderData().setRequestApiKey(messageType.apiKey()).setRequestApiVersion(versionNode.shortValue());

        var testName = String.format(
                "%s,%s,v%d[%s]",
                source,
                messageType,
                versionNode.intValue(),
                Optional.ofNullable(descriptionNode).map(JsonNode::asText).orElse("-")
        );
        var requestConverter = KafkaApiMessageConverter.requestConverterFor(messageType);
        var responseConverter = KafkaApiMessageConverter.responseConverterFor(messageType);
        var request = buildApiMessageTestDef(header.requestApiVersion(), jsonNode.get("request"), requestConverter.reader());
        var response = buildApiMessageTestDef(header.requestApiVersion(), jsonNode.get("response"), responseConverter.reader());
        return new RequestResponseTestDef(testName, messageType, header, request, response);
    }

    private static ApiMessageTestDef buildApiMessageTestDef(short apiVersion, JsonNode node, BiFunction<JsonNode, Short, ApiMessage> readerFunc) {
        if (node != null) {
            var request = readerFunc.apply(node.get("payload"), apiVersion);
            var requestDiff = node.get("diff");
            return new ApiMessageTestDef(request, requestDiff);
        }
        return null;
    }
}
