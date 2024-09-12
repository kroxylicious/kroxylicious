<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#assign
dataClass="${messageSpec.name}Data"
filterClass="${messageSpec.name}Filter"
filterResultClass="${messageSpec.type?lower_case?cap_first}FilterResult"
headerClass="${messageSpec.type?lower_case?cap_first}HeaderData"
filterInvokerClass="${messageSpec.name}FilterInvoker"
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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * An invoker for ${filterClass}. The point is that this invoker knows the type of
 * ${filterClass} to avoid using instanceof/cast, which has a performance issue in
 * the current LTS java (17).
 */
class ${filterInvokerClass} implements

        FilterInvoker {

            private final ${filterClass} filter ;

        ${filterInvokerClass}(${filterClass} filter) {
                    this.filter = filter;
    }

            @Override
                public boolean shouldHandle<#if messageSpec.type?lower_case == 'response'>Response<#else>Request</#if>
            (ApiKeys apiKey,short apiVersion){
                return filter.shouldHandle${messageSpec.name}(apiVersion);
            }

            @Override
                public CompletionStage<${filterResultClass}> on<#if messageSpec.type?lower_case == 'response'>Response<#else>Request</#if>
            (ApiKeys apiKey,short apiVersion, ${headerClass} header, ApiMessage body, FilterContext filterContext){
                return filter.on${messageSpec.name}(apiVersion, header, (${dataClass})body, filterContext);
            }
        }
