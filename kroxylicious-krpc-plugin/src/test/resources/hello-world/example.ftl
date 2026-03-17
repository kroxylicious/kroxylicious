<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
====
    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
====

name: ${inputSpec.name}
type: ${inputSpec.type}
apiKey: ${inputSpec.apiKey}
struct:
  name: ${inputSpec.struct.name}
  hasKeys: ${inputSpec.struct.hasKeys?string('yes', 'no')}
  versions: ${inputSpec.struct.versions}
fields:
<#list inputSpec.struct.fields as f>
    ${f.name}
</#list>
validVersions: ${inputSpec.validVersions}
validVersionsString: ${inputSpec.validVersionsString}
flexibleVersions: ${inputSpec.flexibleVersions}
flexibleVersionsString: ${inputSpec.flexibleVersionsString}
dataClassName: ${inputSpec.dataClassName}
latestVersionUnstable: ${inputSpec.latestVersionUnstable.isPresent()?string('yes', 'no')}
<#assign entityTypes = createEntityTypeSet("GROUP_ID", "TRANSACTIONAL_ID", "TOPIC_NAME")>
entityFields:
<#list entityTypes as et>
  ${et}: ${inputSpec.hasAtLeastOneEntityField(et)?string('yes', 'no')}
</#list>
intersectedVersionsForEntityFields:
<#list entityTypes as et>
  ${et}:<#list inputSpec.intersectedVersionsForEntityFields(et)> <#items as v>${v}<#sep>,</#items></#list>
</#list>
fields:
<#list inputSpec.fields as field>
  name: ${field.name}
  versions: ${field.versions}
  type: ${field.type}
  ignorable: ${field.ignorable?string('yes', 'no')}
</#list>
commonStructs:
<#list inputSpec.commonStructs as commonStruct>
  name: ${commonStruct.name}
</#list>
listeners:
<#if inputSpec.listeners??>
<#list inputSpec.listeners as listener>
  name: ${listener.name()}
</#list>
</#if>
