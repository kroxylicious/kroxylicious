<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->

package ${outputPackage};

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

<#list messageSpecs as messageSpec>
import org.apache.kafka.common.message.${messageSpec.name}Data;
import org.apache.kafka.common.message.${messageSpec.name}DataJsonConverter;
</#list>
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.ApiMessage;

import com.fasterxml.jackson.databind.JsonNode;



public class KafkaApiMessageConverter {

    public record Converter(
            BiFunction<JsonNode, Short, ApiMessage> reader,
            BiFunction<ApiMessage, Short, JsonNode> writer) {
    }

    private static final Map<ApiMessageType, Converter> requestConverters;
    private static final Map<ApiMessageType, Converter> responseConverters;

    static {
        var reqc = new HashMap<ApiMessageType, Converter>();
        var resc = new HashMap<ApiMessageType, Converter>();

<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
        reqc.put(ApiMessageType.${retrieveApiKey(messageSpec)}, new Converter(
                    ${messageSpec.name}DataJsonConverter::read,
                    (o, ver) -> ${messageSpec.name}DataJsonConverter.write(((${messageSpec.name}Data) o), ver)));
    </#if>
    <#if messageSpec.type?lower_case == 'response'>
        resc.put(ApiMessageType.${retrieveApiKey(messageSpec)}, new Converter(
                    ${messageSpec.name}DataJsonConverter::read,
                    (o, ver) -> ${messageSpec.name}DataJsonConverter.write(((${messageSpec.name}Data) o), ver)));
    </#if>
</#list>
        requestConverters = Collections.unmodifiableMap(reqc);
        responseConverters = Collections.unmodifiableMap(resc);
    }

    public static Converter requestConverterFor(ApiMessageType apiMessageType) {
        var converter = requestConverters.get(apiMessageType);
        if (converter == null) {
            throw new IllegalArgumentException("no request converter registered for " + apiMessageType);
        }
        return converter;
    }

    public static Converter responseConverterFor(ApiMessageType apiMessageType) {
        var converter = responseConverters.get(apiMessageType);
        if (converter == null) {
            throw new IllegalArgumentException("no response converter registered for " + apiMessageType);
        }
        return converter;
    }
}
