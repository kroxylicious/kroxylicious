<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#-- @ftlvariable name="outputPackage" type="java.lang.String" -->
<#-- @ftlvariable name="messageSpec" type="io.kroxylicious.krpccodegen.schema.MessageSpec" -->
<#assign
dataClass="${messageSpec.dataClassName}"
conditionClassName="${dataClass?cap_first}Condition"
/>
<#-- there is a mismatch in pluralisation between OffsetsForLeader message spec name and the actual Request type -->
<#if messageSpec.name?starts_with("OffsetForLeader")>
    <#assign requestName = messageSpec.name?replace("Offset", "Offsets") />
<#else>
    <#assign requestName = "${messageSpec.name}" />
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

public class ${conditionClassName} extends Condition

<ApiMessage> {

    private final Predicate<${dataClass}> predicate;

    public ${conditionClassName} (Predicate <${dataClass}> predicate){
        super(new TextDescription("a ${requestName} matching the predicate"));
        this.predicate = predicate;
    }

    @Override
    public boolean matches (ApiMessage apiMessage){
        <#if requestName?matches("^(FetchSnapshot|UpdateMetadata|LeaderAndIsr).*$")>
        if (apiMessage instanceof ${dataClass}) {
            ${dataClass} ${apiMessageVarName} = (${dataClass}) apiMessage;
            return predicate.test(${apiMessageVarName});
        <#else>
        if (apiMessage instanceof ${requestName}) {
            ${requestName} ${apiMessageVarName} =(${requestName}) apiMessage;
            return predicate.test(${apiMessageVarName}.data());
        }
        else if (apiMessage instanceof ${dataClass}) {
            ${dataClass} ${apiMessageVarName} = (${dataClass}) apiMessage;
            return predicate.test(${apiMessageVarName});
        </#if>
    } else {
        return false;
    }
    }

    public static ${conditionClassName} ${requestName?uncap_first}Matching(Predicate <${dataClass}> predicate){
            return new ${conditionClassName}(predicate);
        }
    }
