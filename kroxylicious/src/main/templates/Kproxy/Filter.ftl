<#--

    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements. See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License. You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
<#assign
  dataClass="${messageSpec.name}Data"
  filterClass="${messageSpec.name}Filter"
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
package io.strimzi.kproxy.filter;

import org.apache.kafka.common.message.${messageSpec.name}Data;

/**
 * A stateless filter for ${messageSpec.name}s.
 * The same instance may be invoked on multiple channels.
 */
public interface ${filterClass} extends Krpc${msgType?cap_first}Filter {
<#--
    @Override
    public default void accept(Krpc${msgType?cap_first}Visitor visitor) {
        visitor.visit${filterClass}(this);
    }

    /**
     * Decide whether {@link #on${messageSpec.name}(${messageSpec.name}Data, FilterContext)} should be called on a given ${msgType}.
     * The ${msgType} will only be deserialized if any of the filters in the filter chain request it,
     * otherwise it will be pass up/down stream without being deserialized.
     * @param context The context.
     * @return true if {@link #on${messageSpec.name}(${dataClass}, KrpcFilterContext)} should be called on a given ${messageSpec.type}
     */
    public default boolean shouldIntercept${messageSpec.name}(FilterContext context) {
        return true;
    }
-->
    /**
     * Handle the given {@code ${msgType}},
     * returning the {@code ${messageSpec.name}Data} instance to be passed to the next filter.
     * The implementation may modify the given {@code data} in-place and return it,
     * or instantiate a new one.
     *
     * @param ${msgType} The KRPC message to handle.
     * @param context The context.
     * @return the {@code ${msgType}} to be passed to the next filter.
     * If null is returned then the given {@code ${msgType}} will be used.
     */
    public KrpcFilterState on${messageSpec.name}(${dataClass} ${msgType}, KrpcFilterContext context);

    <#-- TODO this is completely wrong, since it assumes a 1-to-1 relationship between down- and upstream requests
    // (or up- and downstream responses).
    // A plugin actually might need to make multiple upstream requests for a single downstream
    // (e.g. topic encryption with inflation)
    // And combine multiple upstream responses into a single downstream response
    // (e.g. the ack for topic encryption with inflation)
    // So maybe it's the filter context which we use to initiate requests

    // Each plugin effectively has its own state machine, and could return it's expectation
    // of the next event (for flow control and debugging).
    // We'd then need to validate the filterchain
    // (e.g. we can't have a later plugin waiting for input when an earlier plugin
    //  is waiting for a response, because that would be a deadlock)
    -->
}