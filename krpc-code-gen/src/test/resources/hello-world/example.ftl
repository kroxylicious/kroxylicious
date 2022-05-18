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
