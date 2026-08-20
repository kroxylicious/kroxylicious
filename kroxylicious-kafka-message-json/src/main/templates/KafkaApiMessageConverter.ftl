<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->

package ${outputPackage};

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

<#list inputSpecs as inputSpec>
import io.kroxylicious.kafka.common.message.${inputSpec.name}Data;
import io.kroxylicious.kafka.common.message.${inputSpec.name}DataJsonConverter;
</#list>
import io.kroxylicious.kafka.common.message.ApiMessageType;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import com.fasterxml.jackson.databind.JsonNode;



/**
 * Provides converters between the JSON representation of Kafka API messages and the
 * corresponding {@link ApiMessage} instances.
 */
public class KafkaApiMessageConverter {

    /**
     * A pair of functions converting between JSON and {@link ApiMessage} at a given API version.
     *
     * @param reader function converting a JSON node to an {@link ApiMessage} at the given API version
     * @param writer function converting an {@link ApiMessage} to a JSON node at the given API version
     */
    public record Converter(BiFunction<JsonNode, Short, ApiMessage> reader,
                            BiFunction<ApiMessage, Short, JsonNode> writer) {
    }

    private KafkaApiMessageConverter() {
    }

    private static final Map<ApiMessageType, Converter> requestConverters;
    private static final Map<ApiMessageType, Converter> responseConverters;

    static {
        var reqc = new HashMap<ApiMessageType, Converter>();
        var resc = new HashMap<ApiMessageType, Converter>();

<#list inputSpecs as inputSpec>
    <#if inputSpec.type?lower_case == 'request'>
        reqc.put(ApiMessageType.${inputSpec.kafkaApiKeyEnumName}, new Converter(
                    ${inputSpec.name}DataJsonConverter::read,
                (o, ver) -> ${inputSpec.name}DataJsonConverter.write(((${inputSpec.name}Data) o), ver)));
    </#if>
    <#if inputSpec.type?lower_case == 'response'>
        resc.put(ApiMessageType.${inputSpec.kafkaApiKeyEnumName}, new Converter(
                    ${inputSpec.name}DataJsonConverter::read,
                (o, ver) -> ${inputSpec.name}DataJsonConverter.write(((${inputSpec.name}Data) o), ver)));
    </#if>
</#list>
        requestConverters = Collections.unmodifiableMap(reqc);
        responseConverters = Collections.unmodifiableMap(resc);
    }

    /**
     * Returns the converter for the request message of the given API message type.
     *
     * @param apiMessageType the API message type
     * @return the request converter
     * @throws IllegalArgumentException if no request converter is registered for the given type
     */
    public static Converter requestConverterFor(ApiMessageType apiMessageType) {
        var converter = requestConverters.get(apiMessageType);
        if (converter == null) {
            throw new IllegalArgumentException("no request converter registered for " + apiMessageType);
        }
        return converter;
    }

    /**
     * Returns the converter for the response message of the given API message type.
     *
     * @param apiMessageType the API message type
     * @return the response converter
     * @throws IllegalArgumentException if no response converter is registered for the given type
     */
    public static Converter responseConverterFor(ApiMessageType apiMessageType) {
        var converter = responseConverters.get(apiMessageType);
        if (converter == null) {
            throw new IllegalArgumentException("no response converter registered for " + apiMessageType);
        }
        return converter;
    }
}
