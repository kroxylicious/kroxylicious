<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#assign
  dataClass="${messageSpec.name}Data"
  filterClass="${messageSpec.name}Filter"
  filterResultClass="CompletionStage<${messageSpec.type?lower_case?cap_first}FilterResult>"
  headerClass="${messageSpec.type?lower_case?cap_first}HeaderData"
  msgType=messageSpec.type?lower_case
/>
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
package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.${messageSpec.name}Data;
<#if messageSpec.type?lower_case == 'response'>
import org.apache.kafka.common.message.ResponseHeaderData;
<#else>
import org.apache.kafka.common.message.RequestHeaderData;
</#if>

/**
 * A stateless filter for ${messageSpec.name}s.
 * The same instance may be invoked on multiple channels.
 */
public interface ${filterClass} extends KrpcFilter {

    /**
     * Determine if a ${msgType} message of type ${messageSpec.name} should be handled by
     * this filter implementation.
     * returns true then {@code on${messageSpec.name}} is eligible to be invoked with
     * deserialized data, if the message reaches this filter in the chain.
     * @param apiVersion the apiVersion of the message
     * @return true if it should be handled
     */
    default boolean shouldHandle${messageSpec.name}(short apiVersion) {
        return true;
    }

    /**
     * Handle the given {@code ${msgType}},
     * returning the {@code ${messageSpec.name}Data} instance to be passed to the next filter.
     * The implementation may modify the given {@code data} in-place and return it,
     * or instantiate a new one.
     *
     * @param apiVersion the apiVersion of the ${messageSpec.type?lower_case}
     * @param header <#if messageSpec.type?lower_case == 'response'>response<#else>request</#if> header.
     * @param ${msgType} The KRPC message to handle.
     * @param context The context.
     * @return CompletionStage that will yield a <#if messageSpec.type?lower_case == 'response'>{@link ResponseFilterResult}<#else>{@link FilterResult}</#if>
     *         containing the ${messageSpec.type?lower_case} to be forwarded.
     */
     ${filterResultClass} on${messageSpec.name}(short apiVersion, ${headerClass} header, ${dataClass} ${msgType}, KrpcFilterContext context);

}