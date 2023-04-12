<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->

package ${outputPackage};


import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

<#list messageSpecs as messageSpec>
import org.apache.kafka.common.message.${messageSpec.name}Data;
</#list>


/**
* Enumerates all DataClasses
*/
public class DataClasses {

    /**
    * Create an empty DataClasses
    */
    private DataClasses() {

    }

    private static final Map<ApiKeys, Class<? extends ApiMessage>> requestClasses;
    private static final Map<ApiKeys, Class<? extends ApiMessage>> responseClasses;

    static {
        requestClasses = new HashMap<ApiKeys, Class<? extends ApiMessage>>();
        responseClasses = new HashMap<ApiKeys, Class<? extends ApiMessage>>();

<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
        requestClasses.put(ApiKeys.${retrieveApiKey(messageSpec)}, ${messageSpec.name}Data.class);
    </#if>
    <#if messageSpec.type?lower_case == 'response'>
        responseClasses.put(ApiKeys.${retrieveApiKey(messageSpec)}, ${messageSpec.name}Data.class);
    </#if>
</#list>
    }

    /**
    * Get the ApiMessage class per ApiKey for request messages
    * @return ApiKeys to ApiMessage class mappings
    */
    public static Map<ApiKeys, Class<? extends ApiMessage>> getRequestClasses() {
        return Collections.unmodifiableMap(requestClasses);
    }

    /**
    * Get the ApiMessage class per ApiKey for response messages
    * @return ApiKeys to ApiMessage class mappings
    */
    public static Map<ApiKeys, Class<? extends ApiMessage>> getResponseClasses() {
        return Collections.unmodifiableMap(responseClasses);
    }
}
