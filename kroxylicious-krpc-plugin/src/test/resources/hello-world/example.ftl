<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
====
    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
====

name: ${messageSpec.name}
type: ${messageSpec.type}
apiKey: ${messageSpec.apiKey}
struct:
  name: ${messageSpec.struct.name}
  hasKeys: ${messageSpec.struct.hasKeys?string('yes', 'no')}
  versions: ${messageSpec.struct.versions}
fields:
<#list messageSpec.struct.fields as f>
    ${f.name}
</#list>
validVersions: ${messageSpec.validVersions}
validVersionsString: ${messageSpec.validVersionsString}
flexibleVersions: ${messageSpec.flexibleVersions}
flexibleVersionsString: ${messageSpec.flexibleVersionsString}
dataClassName: ${messageSpec.dataClassName}
latestVersionUnstable: ${messageSpec.latestVersionUnstable.isPresent()?string('yes', 'no')}
<#assign entityTypes = createEntityTypeSet("GROUP_ID", "TRANSACTIONAL_ID", "TOPIC_NAME")>
entityFields:
<#list entityTypes as et>
  ${et}: ${messageSpec.hasAtLeastOneEntityField(et)?string('yes', 'no')}
</#list>
intersectedVersionsForEntityFields:
<#list entityTypes as et>
  ${et}:<#list messageSpec.intersectedVersionsForEntityFields(et)> <#items as v>${v}<#sep>,</#items></#list>
</#list>
fields:
<#list messageSpec.fields as field>
  name: ${field.name}
  versions: ${field.versions}
  type: ${field.type}
  ignorable: ${field.ignorable?string('yes', 'no')}
</#list>
commonStructs:
<#list messageSpec.commonStructs as commonStruct>
  name: ${commonStruct.name}
</#list>
listeners:
<#if messageSpec.listeners??>
<#list messageSpec.listeners as listener>
  name: ${listener.name()}
</#list>
</#if>
