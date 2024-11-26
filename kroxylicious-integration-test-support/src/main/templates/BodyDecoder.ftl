<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ${outputPackage};

<#list messageSpecs as messageSpec>
import org.apache.kafka.common.message.${messageSpec.name}Data;
</#list>
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
* Decodes Kafka Readable into an ApiMessage
* <p>Note: this class is automatically generated from a template</p>
*/
public class BodyDecoder {

    /**
    * Creates a BodyDecoder
    */
    private BodyDecoder() {
    }

    /**
    * Decodes Kafka request Readable into an ApiMessage
    * @param apiKey the api key of the message
    * @param apiVersion the api version of the message
    * @param accessor the accessor for the message bytes
    * @return the ApiMessage
    * @throws IllegalArgumentException if an unhandled ApiKey is encountered
    */
    static ApiMessage decodeRequest(ApiKeys apiKey, short apiVersion, ByteBufAccessor accessor) {
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
            case ${retrieveApiKey(messageSpec)} ->
                new ${messageSpec.name}Data(accessor, apiVersion);
    </#if>
</#list>
            default -> throw new IllegalArgumentException("Unsupported RPC " + apiKey);
        };
    }

    /**
    * Decodes Kafka response Readable into an ApiMessage
    * @param apiKey the api key of the message
    * @param apiVersion the api version of the message
    * @param accessor the accessor for the message bytes
    * @return the ApiMessage
    * @throws IllegalArgumentException if an unhandled ApiKey is encountered
    */
    static ApiMessage decodeResponse(ApiKeys apiKey, short apiVersion, ByteBufAccessor accessor) {
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'response'>
        <#if messageSpec.name == 'ApiVersionsResponse'>
            case ${retrieveApiKey(messageSpec)} -> {
                // KIP-511 when the client receives an unsupported version for the ApiVersionResponse, it fails back to version 0
                // Use the same algorithm as https://github.com/apache/kafka/blob/a41c10fd49841381b5207c184a385622094ed440/clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java#L90-L106
                int prev = accessor.readerIndex();
                try {
                    yield new ${messageSpec.name}Data(accessor, apiVersion);
                }
                catch (RuntimeException e) {
                    accessor.readerIndex(prev);
                    if (apiVersion != 0) {
                        yield new ${messageSpec.name}Data(accessor, (short) 0);
                    }
                    else {
                        throw e;
                    }
                }
            }
        <#else>
            case ${retrieveApiKey(messageSpec)} -> new ${messageSpec.name}Data(accessor, apiVersion);
        </#if>
    </#if>
</#list>
            default -> throw new IllegalArgumentException("Unsupported RPC " + apiKey);
        };
    }

}
