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

import io.kroxylicious.proxy.frame.DecodedFrame;
import io.netty.channel.ChannelDuplexHandler;
<#list messageSpecs as messageSpec>
import org.apache.kafka.common.message.${messageSpec.name}Data;
</#list>
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
<#list messageSpecs as messageSpec>
import io.kroxylicious.proxy.filter.${messageSpec.name}Filter;
</#list>

public class FilterInvokerHandler
        extends ChannelDuplexHandler {
    /*
     * Downcasting filter on every invocation of the filter would
     * disturb the Klass.secondary_super_cache which would be harmful to performance.
     * To avoid this we use a field for each filter subtype so no downcast is needed
     * on the hot path (filter invocation).
     * However, initializing a handler does require downcasting.
     */
<#list messageSpecs as messageSpec>
     private final ${messageSpec.name}Filter ${messageSpec.name}Filter;
</#list>
    protected final KrpcFilter filter;

    public FilterInvokerHandler(KrpcFilter filter) {
        super();
        this.filter = filter;
        FilterApis filterApis = FilterApis.forFilter(filter.getClass());
<#list messageSpecs as messageSpec>
        ${messageSpec.name}Filter ${messageSpec.name}Filter = null;
</#list>
        // Rather than a simple if (filter instanceof ProduceRequestFilter) ... else if ...
        // chain we use filterApis to avoid the disturbing the Klass.secondary_super_cache
        // as much as possible.
        for (var ft : ApiType.values()) {
            if (filterApis.consumesAnyVersion(ft)) {
                switch (ft) {
<#list messageSpecs as messageSpec>
                    case ${retrieveApiKey(messageSpec)}_${messageSpec.type?upper_case}:
                        ${messageSpec.name}Filter = (${messageSpec.name}Filter) filter;
                        break;
</#list>
                }
            }
        }
<#list messageSpecs as messageSpec>
        this.${messageSpec.name}Filter = ${messageSpec.name}Filter;
</#list>
    }

    protected boolean consumes(DecodedFrame<?, ?> decodedFrame) {
        return FilterApis.forFilter(filter.getClass())
                .consumesApiVersion(decodedFrame.type(), decodedFrame.apiVersion());
    }

    protected final void invoke(DecodedFrame<?, ?> decodedFrame,
                                KrpcFilterContext filterContext) {
        switch (decodedFrame.type()) {
<#list messageSpecs as messageSpec>
            case ${retrieveApiKey(messageSpec)}_${messageSpec.type?upper_case}:
                ${messageSpec.name}Filter.on${messageSpec.name}((${messageSpec.name}Data) decodedFrame.body(), filterContext);
                return;
</#list>
        }
        throw new IllegalStateException(decodedFrame.type() + " not supported");
    }
}
