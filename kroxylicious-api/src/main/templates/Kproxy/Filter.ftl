<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#assign
dataClass="${messageSpec.name}Data"
filterClass="${messageSpec.name}Filter"
filterResultClass="${messageSpec.type?lower_case?cap_first}FilterResult"
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

import org.apache.kafka.common.message.${dataClass};
import org.apache.kafka.common.message.${headerClass};

/**
 * A stateless filter for ${messageSpec.name}s.
 */
public interface ${filterClass} extends

        Filter {

            /**
             * Determine if a ${msgType} message of type ${messageSpec.name} should be handled by
             * this filter implementation.
             * returns true then {@code on${messageSpec.name}} is eligible to be invoked with
             * deserialized data, if the message reaches this filter in the chain.
             * @param apiVersion the apiVersion of the message
             * @return true if it should be handled
             */
            default
            boolean shouldHandle${messageSpec.name}( short apiVersion){
                return true;
            }

            /**
             * Handle the given {@code header} and {@code ${msgType}} pair, returning the {@code header} and {@code ${msgType}}
             * pair to be passed to the next filter using the ${filterResultClass}.
             * <br/>
             * The implementation may modify the given {@code header} and {@code ${msgType}} in-place, or instantiate a
             * new instances.
             *
             * @param apiVersion the apiVersion of the ${msgType}
             * @param header ${msgType} header.
             * @param ${msgType} The body to handle.
             * @param context The context.
             * @return a non-null CompletionStage that, when complete, will yield a ${filterResultClass} containing the
             *         ${msgType} to be forwarded.
             * @see io.kroxylicious.proxy.filter Creating Filter Result objects
             * @see io.kroxylicious.proxy.filter  Thread Safety
             */
            CompletionStage<${filterResultClass}> on${messageSpec.name}(
            short apiVersion, ${headerClass} header, ${dataClass} ${msgType},FilterContext context);

        }
