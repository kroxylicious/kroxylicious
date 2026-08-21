<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#assign
dataClass="${inputSpec.dataClassName}"
conditionClassName="${dataClass?cap_first}Condition"
/>
<#-- there is a mismatch in pluralisation between OffsetsForLeader message spec name and the actual Request type -->
<#if inputSpec.name?starts_with("OffsetForLeader")>
    <#assign requestName = inputSpec.name?replace("Offset", "Offsets") />
<#else>
    <#assign requestName = "${inputSpec.name}" />
</#if>
<#assign apiMessageVarName="${requestName?uncap_first}" />
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${outputPackage};

import java.util.function.Predicate;

import org.apache.kafka.common.message.${dataClass};
<#if requestName?matches("^(FetchSnapshot|UpdateMetadata|LeaderAndIsr).*$")>
<#--Suppress the import for the final AbstractControl implementations (FetchSnapshot etc)-->
<#else>
import org.apache.kafka.common.requests.${requestName};
</#if>
import org.apache.kafka.common.protocol.ApiMessage;

import org.assertj.core.api.Condition;
import org.assertj.core.description.TextDescription;

/**
 * An AssertJ {@link Condition} that matches {@link ${dataClass}} messages satisfying a predicate.
 */
public class ${conditionClassName} extends Condition<ApiMessage> {

    private final Predicate<${dataClass}> predicate;

    /**
     * Constructs a condition matching ${requestName} messages that satisfy the given predicate.
     *
     * @param predicate the predicate the message must satisfy
     */
    public ${conditionClassName} (Predicate<${dataClass}> predicate) {
        super(new TextDescription("a ${requestName} matching the predicate"));
        this.predicate=predicate;
    }

    @Override
    public boolean matches(ApiMessage apiMessage) {
        <#if requestName?matches("^(FetchSnapshot|UpdateMetadata|LeaderAndIsr).*$")>
        if (apiMessage instanceof ${dataClass}) {
            ${dataClass} ${apiMessageVarName} = (${dataClass}) apiMessage;
            return predicate.test(${apiMessageVarName});
        <#else>
        if (apiMessage instanceof ${requestName}) {
            ${requestName} ${apiMessageVarName} = (${requestName}) apiMessage;
            return predicate.test(${apiMessageVarName}.data());
        }
        else if (apiMessage instanceof ${dataClass}) {
            ${dataClass} ${apiMessageVarName} = (${dataClass}) apiMessage;
            return predicate.test(${apiMessageVarName});
        </#if>
        }
        else {
            return false;
        }
    }

    /**
     * Creates a condition matching ${requestName} messages that satisfy the given predicate.
     *
     * @param predicate the predicate the message must satisfy
     * @return the condition
     */
    public static ${conditionClassName} ${requestName?uncap_first}Matching(Predicate<${dataClass}> predicate) {
        return new ${conditionClassName}(predicate);
    }
}