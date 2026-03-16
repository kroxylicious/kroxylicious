<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#assign
  dataClass="${inputSpec.name}Data"
  filterClass="${inputSpec.name}Filter"
  msgType=inputSpec.type?lower_case
/>
====
    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
====

package ${outputPackage};

import org.apache.kafka.common.message.${inputSpec.name}Data;

/**
 * A stateless filter for ${inputSpec.name}s.
 * The same instance may be invoked on multiple channels.
 */
public interface ${filterClass} extends Krpc${msgType?cap_first}Filter {
    /**
     * Handle the given {@code ${msgType}},
     * returning the {@code ${inputSpec.name}Data} instance to be passed to the next filter.
     * The implementation may modify the given {@code data} in-place and return it,
     * or instantiate a new one.
     *
     * @param ${msgType} The KRPC message to handle.
     * @param context The context.
     * @return the {@code ${msgType}} to be passed to the next filter.
     * If null is returned then the given {@code ${msgType}} will be used.
     */
    public KrpcFilterState on${inputSpec.name}(${dataClass} ${msgType}, KrpcFilterContext context);

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