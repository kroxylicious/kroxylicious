<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#assign
dataClass = "${messageSpec.name}Data"
/>
<#macro fieldName field>${field.name?uncap_first}</#macro>
<#macro fieldType field>
    <#compress>
        <#if field.type == 'int32'>
int
        <#elseif field.type == 'int64'>
long
        <#elseif field.type == 'bool'>
boolean
        <#elseif field.type.isArray && !field.mapKey>
            ${field.type.elementType}Collection
        <#elseif field.type.isArray>
List<${field.name}>
        <#else>
UNKNOWN - ${field.name}
        </#if>
    </#compress>
</#macro>

<#macro fieldDefault field>
    <#compress>
        <#if field.type.isArray && !field.mapKey>
new ${field.type.elementType}Collection(0)
        <#else>
            ${field.defaultString}
        </#if>
    </#compress>
</#macro>

<#macro generateSchemas name, struct, parentVersions>
    <#assign messageFlexibleVersions = flexibleVersions />
    <#local versions = parentVersions.intersect(struct.versions) />
    <#list struct.fields as field>
    <#-- We actually don't want to recurse here, because where Kafka's compile builds a map
         of schemas separately from generating the code, we're not doing that, at least right now -->
        <#if field.type.isStructArray>
            <@generateSchemas field.type.elementType, structRegistry.findStruct(field), versions />
        <#elseif field.type.isStruct>
            <@generateSchemas field.type, structRegistry.findStruct(field), versions />
        </#if>
    </#list>

    <#list versions as v>
        <@generateSchemaForVersion struct=struct version=v />
    </#list>
</#macro>

<#macro generateSchemaForVersion struct, version>
    public static final Schema SCHEMA_${version} =
            new Schema(
<#list struct.fields as field>
    <#if !field.versions.contains(version) || field.taggedVersions.contains(version)>
        <#continue/>
    </#if>
            new Field("${field.name}", <@schemaType type=field.type version=version fieldFlexibleVersions=field.flexibleVersions.orElse(messageFlexibleVersions)/>,
                      "${field.about}")<#sep>,</#sep><#if !field?has_next && messageFlexibleVersions.contains(version)>,
            TaggedFieldsSection.of(
            )</#if>
</#list>
            );
</#macro>

<#macro schemaType type version fieldFlexibleVersions>
    <#compress>
        <#if type == 'int32'>
    Type.INT32
        <#elseif type == 'int16'>
    Type.INT16
        <#elseif type == 'int8'>
    Type.INT8
        <#elseif type == 'int64'>
    Type.INT64
        <#elseif type == 'bool'>
    Type.BOOLEAN
        <#elseif type == 'string'>
    Type.STRING
        <#elseif type.isArray && fieldFlexibleVersions.contains(version)>
    new

        CompactArrayOf(<@schemaType type=type.elementType version=version fieldFlexibleVersions=fieldFlexibleVersions/>)
        <#elseif type.isArray>
    new

        ArrayOf(<@schemaType type=type.elementType version=version fieldFlexibleVersions=fieldFlexibleVersions/>)
        <#elseif type.isStruct>
            ${type}.SCHEMA_${version}
        <#else>
    UNKNOWN schemaType ${type}
        </#if>
    </#compress>
</#macro>
====
    Copyright Kroxylicious
Authors .

    Licensed under
the Apache
Software License
version 2.0,
available at
http://www.apache.org/licenses/LICENSE-2.0
        ====

        // THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

        package org.apache.kafka.common.message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.ImplicitLinkedHashMultiCollection;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;

public class ${dataClass} implements

ApiMessage {

<#-- field declarations -->
<#list messageSpec.struct.fields as f>
    <@fieldType field=f/> <@fieldName field=f/>;
</#list>
    private List<RawTaggedField> _unknownTaggedFields;

<#-- schemas -->
<#--
<#list structRegistry.commonStructs() as cstruct>
    <@generateSchemas name=cstruct.name struct=cstruct parentVersions=cstruct.versions/>
</#list>
    <@generateSchemas name=dataClassName struct=struct parentVersions=struct.versions/>
-->

    public static final Schema[] SCHEMAS = new Schema[]{
<#list messageSpec.validVersions as v>
        SCHEMA_${v}<#sep>,</#sep>
</#list>
    };

    public static final short LOWEST_SUPPORTED_VERSION = ${messageSpec.validVersions.lowest};
    public static final short HIGHEST_SUPPORTED_VERSION = ${messageSpec.validVersions.highest};

<#-- constructors -->
    public ${dataClass}(Readable _readable,short _version){
        read(_readable, _version);
    }

    public ${dataClass}() {
<#list messageSpec.struct.fields as f>
        this.<@fieldName field=f/> = <@fieldDefault field=f/>;
</#list>
    }

    /** The API key */
    @Override
    public short apiKey () {
        return ${messageSpec.apiKey.orElse(-1)};
    }

    /** @return the lowest valid version for this RPC. */
    @Override
    public short lowestSupportedVersion () {
        return ${messageSpec.validVersions.lowest};
    }

    /** @return the highest valid version for this message. */
    @Override
    public short highestSupportedVersion () {
        return ${messageSpec.validVersions.highest};
    }

    @Override
    public void read (Readable _readable,short _version){
        // TODO
    }

    @Override
    public void write (Writeable _writeable, ObjectSerializationCache _cache,short _version){
        // TODO
    }

    @Override
    public void addSize (MessageSizeAccumulator _size, ObjectSerializationCache _cache,short _version){
        // TODO
    }

    @Override
    public boolean equals (Object obj){
        if (!(obj instanceof ${dataClass}))
            return false;
    ${dataClass} other = (${dataClass}) obj;
<#list messageSpec.struct.fields as f>
    <#if f.type.canBeNullable>
        if (this.<@fieldName field=f/> == null) {
            if (other.<@fieldName field=f/> != null)
                return false;
        } else {
            if (!this.<@fieldName field=f/>.equals(other.<@fieldName field=f/>))return false;
        }
    <#else>
        if (<@fieldName field=f/> !=other.<@fieldName field=f/>)return false;
    </#if>
</#list>
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }

    @Override
    public int hashCode () {
        int hashCode = 0;
<#list messageSpec.struct.fields as f>
        hashCode = 31 * hashCode + <#compress>
    <#if f.type.canBeNullable>
        (<@fieldName field=f/> == null ? 0 : <@fieldName field=f/>.hashCode())
    <#elseif f.type == 'int32'>
        <@fieldName field=f/>
    <#elseif f.type == 'int16'>
        <@fieldName field=f/>
    <#elseif f.type == 'int8'>
        <@fieldName field=f/>
    <#elseif f.type == 'bool'>
        (<@fieldName field=f/> ? 1231 : 1237)
    <#else>
        UNSUPPORTED TYPE ${f.type}
    </#if>
</#compress>;
</#list>
        return hashCode;
    }

    @Override
    public ${dataClass} duplicate() {
    ${dataClass} _duplicate = new ${dataClass}();
<#list messageSpec.struct.fields as f>
    <#if f.type.isArray>
        ${f.type.elementType}Collection
        new${f.name?cap_first} = new ${f.type.elementType}Collection(<@fieldName field=f/>.size());
        for (${f.type.elementType} _element: <@fieldName field=f/>) {
            new${f.name?cap_first}.add(_element.duplicate());
        }
        _duplicate.<@fieldName field=f/> = new${f.name?cap_first};
    <#else>
        _duplicate.<@fieldName field=f/> = <@fieldName field=f/>;
    </#if>
</#list>
        return _duplicate;
    }

    @Override
    public String toString () {
        return "${dataClass}("
               // TODO
               + ")";
    }

<#-- getters -->
<#list messageSpec.struct.fields as f>
    /**
     * Gets the ${f.name}. 
     * ${f.about}
     * @return the ${f.name}.
     */
    public <@fieldType field=f/> ${f.name?lower_case}() {
    return this.<@fieldName field=f/>;
}
</#list>

    @Override
    public List<RawTaggedField> unknownTaggedFields () {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }

<#-- setters -->
<#list messageSpec.struct.fields as f>
    /**
     * Sets the ${f.name}. 
     * ${f.about}
     * @param ${f.name} the ${f.name}.
     */
    public ${messageSpec.struct.name}Data set${f.name?cap_first}(<@fieldType field=f/> <@fieldName field=f/>) {
    this.<@fieldName field=f/> = <@fieldName field=f/>;
    return this;
}
</#list>

    // TODO nested classes

}
