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

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package org.apache.kafka.common.message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.Uuid;
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
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.ImplicitLinkedHashMultiCollection;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class FetchResponseData implements ApiMessage {
    int throttleTimeMs;
    short errorCode;
    int sessionId;
    List<FetchableTopicResponse> responses;
    NodeEndpointCollection nodeEndpoints;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_0), "The response topics.")
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_0), "The response topics.")
        );
    
    public static final Schema SCHEMA_2 = SCHEMA_1;
    
    public static final Schema SCHEMA_3 = SCHEMA_2;
    
    public static final Schema SCHEMA_4 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_4), "The response topics.")
        );
    
    public static final Schema SCHEMA_5 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_5), "The response topics.")
        );
    
    public static final Schema SCHEMA_6 = SCHEMA_5;
    
    public static final Schema SCHEMA_7 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The top level response error code."),
            new Field("session_id", Type.INT32, "The fetch session ID, or 0 if this is not part of a fetch session."),
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_5), "The response topics.")
        );
    
    public static final Schema SCHEMA_8 = SCHEMA_7;
    
    public static final Schema SCHEMA_9 = SCHEMA_8;
    
    public static final Schema SCHEMA_10 = SCHEMA_9;
    
    public static final Schema SCHEMA_11 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The top level response error code."),
            new Field("session_id", Type.INT32, "The fetch session ID, or 0 if this is not part of a fetch session."),
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_11), "The response topics.")
        );
    
    public static final Schema SCHEMA_12 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The top level response error code."),
            new Field("session_id", Type.INT32, "The fetch session ID, or 0 if this is not part of a fetch session."),
            new Field("responses", new CompactArrayOf(FetchableTopicResponse.SCHEMA_12), "The response topics."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_13 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The top level response error code."),
            new Field("session_id", Type.INT32, "The fetch session ID, or 0 if this is not part of a fetch session."),
            new Field("responses", new CompactArrayOf(FetchableTopicResponse.SCHEMA_13), "The response topics."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_14 = SCHEMA_13;
    
    public static final Schema SCHEMA_15 = SCHEMA_14;
    
    public static final Schema SCHEMA_16 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The top level response error code."),
            new Field("session_id", Type.INT32, "The fetch session ID, or 0 if this is not part of a fetch session."),
            new Field("responses", new CompactArrayOf(FetchableTopicResponse.SCHEMA_13), "The response topics."),
            TaggedFieldsSection.of(
                0, new Field("node_endpoints", new CompactArrayOf(NodeEndpoint.SCHEMA_16), "Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.")
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2,
        SCHEMA_3,
        SCHEMA_4,
        SCHEMA_5,
        SCHEMA_6,
        SCHEMA_7,
        SCHEMA_8,
        SCHEMA_9,
        SCHEMA_10,
        SCHEMA_11,
        SCHEMA_12,
        SCHEMA_13,
        SCHEMA_14,
        SCHEMA_15,
        SCHEMA_16
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 16;
    
    public FetchResponseData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public FetchResponseData() {
        this.throttleTimeMs = 0;
        this.errorCode = (short) 0;
        this.sessionId = 0;
        this.responses = new ArrayList<FetchableTopicResponse>(0);
        this.nodeEndpoints = new NodeEndpointCollection(0);
    }
    
    @Override
    public short apiKey() {
        return 1;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 16;
    }
    
    @Override
    public final void read(Readable _readable, short _version) {
        if (_version >= 1) {
            this.throttleTimeMs = _readable.readInt();
        } else {
            this.throttleTimeMs = 0;
        }
        if (_version >= 7) {
            this.errorCode = _readable.readShort();
        } else {
            this.errorCode = (short) 0;
        }
        if (_version >= 7) {
            this.sessionId = _readable.readInt();
        } else {
            this.sessionId = 0;
        }
        {
            if (_version >= 12) {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field responses was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<FetchableTopicResponse> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new FetchableTopicResponse(_readable, _version));
                    }
                    this.responses = newCollection;
                }
            } else {
                int arrayLength;
                arrayLength = _readable.readInt();
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field responses was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<FetchableTopicResponse> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new FetchableTopicResponse(_readable, _version));
                    }
                    this.responses = newCollection;
                }
            }
        }
        {
            this.nodeEndpoints = new NodeEndpointCollection(0);
        }
        this._unknownTaggedFields = null;
        if (_version >= 12) {
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    case 0: {
                        if (_version >= 16) {
                            int arrayLength;
                            arrayLength = _readable.readUnsignedVarint() - 1;
                            if (arrayLength < 0) {
                                throw new RuntimeException("non-nullable field nodeEndpoints was serialized as null");
                            } else {
                                if (arrayLength > _readable.remaining()) {
                                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                                }
                                NodeEndpointCollection newCollection = new NodeEndpointCollection(arrayLength);
                                for (int i = 0; i < arrayLength; i++) {
                                    newCollection.add(new NodeEndpoint(_readable, _version));
                                }
                                this.nodeEndpoints = newCollection;
                            }
                            break;
                        } else {
                            throw new RuntimeException("Tag 0 is not valid for version " + _version);
                        }
                    }
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        if (_version >= 1) {
            _writable.writeInt(throttleTimeMs);
        }
        if (_version >= 7) {
            _writable.writeShort(errorCode);
        }
        if (_version >= 7) {
            _writable.writeInt(sessionId);
        } else {
            if (this.sessionId != 0) {
                throw new UnsupportedVersionException("Attempted to write a non-default sessionId at version " + _version);
            }
        }
        if (_version >= 12) {
            _writable.writeUnsignedVarint(responses.size() + 1);
            for (FetchableTopicResponse responsesElement : responses) {
                responsesElement.write(_writable, _cache, _version);
            }
        } else {
            _writable.writeInt(responses.size());
            for (FetchableTopicResponse responsesElement : responses) {
                responsesElement.write(_writable, _cache, _version);
            }
        }
        if (_version >= 16) {
            if (!this.nodeEndpoints.isEmpty()) {
                _numTaggedFields++;
            }
        } else {
            if (!this.nodeEndpoints.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default nodeEndpoints at version " + _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 12) {
            _writable.writeUnsignedVarint(_numTaggedFields);
            if (_version >= 16) {
                {
                    if (!this.nodeEndpoints.isEmpty()) {
                        _writable.writeUnsignedVarint(0);
                        _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.nodeEndpoints));
                        _writable.writeUnsignedVarint(nodeEndpoints.size() + 1);
                        for (NodeEndpoint nodeEndpointsElement : nodeEndpoints) {
                            nodeEndpointsElement.write(_writable, _cache, _version);
                        }
                    }
                }
            }
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        if (_version >= 1) {
            _size.addBytes(4);
        }
        if (_version >= 7) {
            _size.addBytes(2);
        }
        if (_version >= 7) {
            _size.addBytes(4);
        }
        {
            if (_version >= 12) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(responses.size() + 1));
            } else {
                _size.addBytes(4);
            }
            for (FetchableTopicResponse responsesElement : responses) {
                responsesElement.addSize(_size, _cache, _version);
            }
        }
        if (_version >= 16) {
            {
                if (!this.nodeEndpoints.isEmpty()) {
                    _numTaggedFields++;
                    _size.addBytes(1);
                    int _sizeBeforeArray = _size.totalSize();
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(nodeEndpoints.size() + 1));
                    for (NodeEndpoint nodeEndpointsElement : nodeEndpoints) {
                        nodeEndpointsElement.addSize(_size, _cache, _version);
                    }
                    int _arraySize = _size.totalSize() - _sizeBeforeArray;
                    _cache.setArraySizeInBytes(nodeEndpoints, _arraySize);
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));
                }
            }
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                _size.addBytes(_field.size());
            }
        }
        if (_version >= 12) {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FetchResponseData)) return false;
        FetchResponseData other = (FetchResponseData) obj;
        if (throttleTimeMs != other.throttleTimeMs) return false;
        if (errorCode != other.errorCode) return false;
        if (sessionId != other.sessionId) return false;
        if (this.responses == null) {
            if (other.responses != null) return false;
        } else {
            if (!this.responses.equals(other.responses)) return false;
        }
        if (this.nodeEndpoints == null) {
            if (other.nodeEndpoints != null) return false;
        } else {
            if (!this.nodeEndpoints.equals(other.nodeEndpoints)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + throttleTimeMs;
        hashCode = 31 * hashCode + errorCode;
        hashCode = 31 * hashCode + sessionId;
        hashCode = 31 * hashCode + (responses == null ? 0 : responses.hashCode());
        hashCode = 31 * hashCode + (nodeEndpoints == null ? 0 : nodeEndpoints.hashCode());
        return hashCode;
    }
    
    @Override
    public FetchResponseData duplicate() {
        FetchResponseData _duplicate = new FetchResponseData();
        _duplicate.throttleTimeMs = throttleTimeMs;
        _duplicate.errorCode = errorCode;
        _duplicate.sessionId = sessionId;
        ArrayList<FetchableTopicResponse> newResponses = new ArrayList<FetchableTopicResponse>(responses.size());
        for (FetchableTopicResponse _element : responses) {
            newResponses.add(_element.duplicate());
        }
        _duplicate.responses = newResponses;
        NodeEndpointCollection newNodeEndpoints = new NodeEndpointCollection(nodeEndpoints.size());
        for (NodeEndpoint _element : nodeEndpoints) {
            newNodeEndpoints.add(_element.duplicate());
        }
        _duplicate.nodeEndpoints = newNodeEndpoints;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "FetchResponseData("
            + "throttleTimeMs=" + throttleTimeMs
            + ", errorCode=" + errorCode
            + ", sessionId=" + sessionId
            + ", responses=" + MessageUtil.deepToString(responses.iterator())
            + ", nodeEndpoints=" + MessageUtil.deepToString(nodeEndpoints.iterator())
            + ")";
    }
    
    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }
    
    public short errorCode() {
        return this.errorCode;
    }
    
    public int sessionId() {
        return this.sessionId;
    }
    
    public List<FetchableTopicResponse> responses() {
        return this.responses;
    }
    
    public NodeEndpointCollection nodeEndpoints() {
        return this.nodeEndpoints;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public FetchResponseData setThrottleTimeMs(int v) {
        this.throttleTimeMs = v;
        return this;
    }
    
    public FetchResponseData setErrorCode(short v) {
        this.errorCode = v;
        return this;
    }
    
    public FetchResponseData setSessionId(int v) {
        this.sessionId = v;
        return this;
    }
    
    public FetchResponseData setResponses(List<FetchableTopicResponse> v) {
        this.responses = v;
        return this;
    }
    
    public FetchResponseData setNodeEndpoints(NodeEndpointCollection v) {
        this.nodeEndpoints = v;
        return this;
    }
    
    public static class FetchableTopicResponse implements Message {
        String topic;
        Uuid topicId;
        List<PartitionData> partitions;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("topic", Type.STRING, "The topic name."),
                new Field("partitions", new ArrayOf(PartitionData.SCHEMA_0), "The topic partitions.")
            );
        
        public static final Schema SCHEMA_1 = SCHEMA_0;
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema SCHEMA_3 = SCHEMA_2;
        
        public static final Schema SCHEMA_4 =
            new Schema(
                new Field("topic", Type.STRING, "The topic name."),
                new Field("partitions", new ArrayOf(PartitionData.SCHEMA_4), "The topic partitions.")
            );
        
        public static final Schema SCHEMA_5 =
            new Schema(
                new Field("topic", Type.STRING, "The topic name."),
                new Field("partitions", new ArrayOf(PartitionData.SCHEMA_5), "The topic partitions.")
            );
        
        public static final Schema SCHEMA_6 = SCHEMA_5;
        
        public static final Schema SCHEMA_7 = SCHEMA_6;
        
        public static final Schema SCHEMA_8 = SCHEMA_7;
        
        public static final Schema SCHEMA_9 = SCHEMA_8;
        
        public static final Schema SCHEMA_10 = SCHEMA_9;
        
        public static final Schema SCHEMA_11 =
            new Schema(
                new Field("topic", Type.STRING, "The topic name."),
                new Field("partitions", new ArrayOf(PartitionData.SCHEMA_11), "The topic partitions.")
            );
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("topic", Type.COMPACT_STRING, "The topic name."),
                new Field("partitions", new CompactArrayOf(PartitionData.SCHEMA_12), "The topic partitions."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_13 =
            new Schema(
                new Field("topic_id", Type.UUID, "The unique topic ID"),
                new Field("partitions", new CompactArrayOf(PartitionData.SCHEMA_12), "The topic partitions."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_14 = SCHEMA_13;
        
        public static final Schema SCHEMA_15 = SCHEMA_14;
        
        public static final Schema SCHEMA_16 = SCHEMA_15;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2,
            SCHEMA_3,
            SCHEMA_4,
            SCHEMA_5,
            SCHEMA_6,
            SCHEMA_7,
            SCHEMA_8,
            SCHEMA_9,
            SCHEMA_10,
            SCHEMA_11,
            SCHEMA_12,
            SCHEMA_13,
            SCHEMA_14,
            SCHEMA_15,
            SCHEMA_16
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 16;
        
        public FetchableTopicResponse(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public FetchableTopicResponse() {
            this.topic = "";
            this.topicId = Uuid.ZERO_UUID;
            this.partitions = new ArrayList<PartitionData>(0);
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 16;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of FetchableTopicResponse");
            }
            if (_version <= 12) {
                int length;
                if (_version >= 12) {
                    length = _readable.readUnsignedVarint() - 1;
                } else {
                    length = _readable.readShort();
                }
                if (length < 0) {
                    throw new RuntimeException("non-nullable field topic was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field topic had invalid length " + length);
                } else {
                    this.topic = _readable.readString(length);
                }
            } else {
                this.topic = "";
            }
            if (_version >= 13) {
                this.topicId = _readable.readUuid();
            } else {
                this.topicId = Uuid.ZERO_UUID;
            }
            {
                if (_version >= 12) {
                    int arrayLength;
                    arrayLength = _readable.readUnsignedVarint() - 1;
                    if (arrayLength < 0) {
                        throw new RuntimeException("non-nullable field partitions was serialized as null");
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<PartitionData> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new PartitionData(_readable, _version));
                        }
                        this.partitions = newCollection;
                    }
                } else {
                    int arrayLength;
                    arrayLength = _readable.readInt();
                    if (arrayLength < 0) {
                        throw new RuntimeException("non-nullable field partitions was serialized as null");
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<PartitionData> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new PartitionData(_readable, _version));
                        }
                        this.partitions = newCollection;
                    }
                }
            }
            this._unknownTaggedFields = null;
            if (_version >= 12) {
                int _numTaggedFields = _readable.readUnsignedVarint();
                for (int _i = 0; _i < _numTaggedFields; _i++) {
                    int _tag = _readable.readUnsignedVarint();
                    int _size = _readable.readUnsignedVarint();
                    switch (_tag) {
                        default:
                            this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                            break;
                    }
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version <= 12) {
                {
                    byte[] _stringBytes = _cache.getSerializedValue(topic);
                    if (_version >= 12) {
                        _writable.writeUnsignedVarint(_stringBytes.length + 1);
                    } else {
                        _writable.writeShort((short) _stringBytes.length);
                    }
                    _writable.writeByteArray(_stringBytes);
                }
            }
            if (_version >= 13) {
                _writable.writeUuid(topicId);
            }
            if (_version >= 12) {
                _writable.writeUnsignedVarint(partitions.size() + 1);
                for (PartitionData partitionsElement : partitions) {
                    partitionsElement.write(_writable, _cache, _version);
                }
            } else {
                _writable.writeInt(partitions.size());
                for (PartitionData partitionsElement : partitions) {
                    partitionsElement.write(_writable, _cache, _version);
                }
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 12) {
                _writable.writeUnsignedVarint(_numTaggedFields);
                _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of FetchableTopicResponse");
            }
            if (_version <= 12) {
                {
                    byte[] _stringBytes = topic.getBytes(StandardCharsets.UTF_8);
                    if (_stringBytes.length > 0x7fff) {
                        throw new RuntimeException("'topic' field is too long to be serialized");
                    }
                    _cache.cacheSerializedValue(topic, _stringBytes);
                    if (_version >= 12) {
                        _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
                    } else {
                        _size.addBytes(_stringBytes.length + 2);
                    }
                }
            }
            if (_version >= 13) {
                _size.addBytes(16);
            }
            {
                if (_version >= 12) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(partitions.size() + 1));
                } else {
                    _size.addBytes(4);
                }
                for (PartitionData partitionsElement : partitions) {
                    partitionsElement.addSize(_size, _cache, _version);
                }
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            if (_version >= 12) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof FetchableTopicResponse)) return false;
            FetchableTopicResponse other = (FetchableTopicResponse) obj;
            if (this.topic == null) {
                if (other.topic != null) return false;
            } else {
                if (!this.topic.equals(other.topic)) return false;
            }
            if (!this.topicId.equals(other.topicId)) return false;
            if (this.partitions == null) {
                if (other.partitions != null) return false;
            } else {
                if (!this.partitions.equals(other.partitions)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (topic == null ? 0 : topic.hashCode());
            hashCode = 31 * hashCode + topicId.hashCode();
            hashCode = 31 * hashCode + (partitions == null ? 0 : partitions.hashCode());
            return hashCode;
        }
        
        @Override
        public FetchableTopicResponse duplicate() {
            FetchableTopicResponse _duplicate = new FetchableTopicResponse();
            _duplicate.topic = topic;
            _duplicate.topicId = topicId;
            ArrayList<PartitionData> newPartitions = new ArrayList<PartitionData>(partitions.size());
            for (PartitionData _element : partitions) {
                newPartitions.add(_element.duplicate());
            }
            _duplicate.partitions = newPartitions;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "FetchableTopicResponse("
                + "topic=" + ((topic == null) ? "null" : "'" + topic.toString() + "'")
                + ", topicId=" + topicId.toString()
                + ", partitions=" + MessageUtil.deepToString(partitions.iterator())
                + ")";
        }
        
        public String topic() {
            return this.topic;
        }
        
        public Uuid topicId() {
            return this.topicId;
        }
        
        public List<PartitionData> partitions() {
            return this.partitions;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public FetchableTopicResponse setTopic(String v) {
            this.topic = v;
            return this;
        }
        
        public FetchableTopicResponse setTopicId(Uuid v) {
            this.topicId = v;
            return this;
        }
        
        public FetchableTopicResponse setPartitions(List<PartitionData> v) {
            this.partitions = v;
            return this;
        }
    }
    
    public static class PartitionData implements Message {
        int partitionIndex;
        short errorCode;
        long highWatermark;
        long lastStableOffset;
        long logStartOffset;
        EpochEndOffset divergingEpoch;
        LeaderIdAndEpoch currentLeader;
        SnapshotId snapshotId;
        List<AbortedTransaction> abortedTransactions;
        int preferredReadReplica;
        BaseRecords records;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("high_watermark", Type.INT64, "The current high water mark."),
                new Field("records", Type.RECORDS, "The record data.")
            );
        
        public static final Schema SCHEMA_1 = SCHEMA_0;
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema SCHEMA_3 = SCHEMA_2;
        
        public static final Schema SCHEMA_4 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("high_watermark", Type.INT64, "The current high water mark."),
                new Field("last_stable_offset", Type.INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
                new Field("aborted_transactions", ArrayOf.nullable(AbortedTransaction.SCHEMA_4), "The aborted transactions."),
                new Field("records", Type.RECORDS, "The record data.")
            );
        
        public static final Schema SCHEMA_5 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("high_watermark", Type.INT64, "The current high water mark."),
                new Field("last_stable_offset", Type.INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
                new Field("log_start_offset", Type.INT64, "The current log start offset."),
                new Field("aborted_transactions", ArrayOf.nullable(AbortedTransaction.SCHEMA_4), "The aborted transactions."),
                new Field("records", Type.RECORDS, "The record data.")
            );
        
        public static final Schema SCHEMA_6 = SCHEMA_5;
        
        public static final Schema SCHEMA_7 = SCHEMA_6;
        
        public static final Schema SCHEMA_8 = SCHEMA_7;
        
        public static final Schema SCHEMA_9 = SCHEMA_8;
        
        public static final Schema SCHEMA_10 = SCHEMA_9;
        
        public static final Schema SCHEMA_11 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("high_watermark", Type.INT64, "The current high water mark."),
                new Field("last_stable_offset", Type.INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
                new Field("log_start_offset", Type.INT64, "The current log start offset."),
                new Field("aborted_transactions", ArrayOf.nullable(AbortedTransaction.SCHEMA_4), "The aborted transactions."),
                new Field("preferred_read_replica", Type.INT32, "The preferred read replica for the consumer to use on its next fetch request"),
                new Field("records", Type.RECORDS, "The record data.")
            );
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("high_watermark", Type.INT64, "The current high water mark."),
                new Field("last_stable_offset", Type.INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
                new Field("log_start_offset", Type.INT64, "The current log start offset."),
                new Field("aborted_transactions", CompactArrayOf.nullable(AbortedTransaction.SCHEMA_12), "The aborted transactions."),
                new Field("preferred_read_replica", Type.INT32, "The preferred read replica for the consumer to use on its next fetch request"),
                new Field("records", Type.COMPACT_RECORDS, "The record data."),
                TaggedFieldsSection.of(
                    0, new Field("diverging_epoch", EpochEndOffset.SCHEMA_12, "In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge"),
                    1, new Field("current_leader", LeaderIdAndEpoch.SCHEMA_12, ""),
                    2, new Field("snapshot_id", SnapshotId.SCHEMA_12, "In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.")
                )
            );
        
        public static final Schema SCHEMA_13 = SCHEMA_12;
        
        public static final Schema SCHEMA_14 = SCHEMA_13;
        
        public static final Schema SCHEMA_15 = SCHEMA_14;
        
        public static final Schema SCHEMA_16 = SCHEMA_15;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2,
            SCHEMA_3,
            SCHEMA_4,
            SCHEMA_5,
            SCHEMA_6,
            SCHEMA_7,
            SCHEMA_8,
            SCHEMA_9,
            SCHEMA_10,
            SCHEMA_11,
            SCHEMA_12,
            SCHEMA_13,
            SCHEMA_14,
            SCHEMA_15,
            SCHEMA_16
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 16;
        
        public PartitionData(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public PartitionData() {
            this.partitionIndex = 0;
            this.errorCode = (short) 0;
            this.highWatermark = 0L;
            this.lastStableOffset = -1L;
            this.logStartOffset = -1L;
            this.divergingEpoch = new EpochEndOffset();
            this.currentLeader = new LeaderIdAndEpoch();
            this.snapshotId = new SnapshotId();
            this.abortedTransactions = new ArrayList<AbortedTransaction>(0);
            this.preferredReadReplica = -1;
            this.records = null;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 16;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of PartitionData");
            }
            this.partitionIndex = _readable.readInt();
            this.errorCode = _readable.readShort();
            this.highWatermark = _readable.readLong();
            if (_version >= 4) {
                this.lastStableOffset = _readable.readLong();
            } else {
                this.lastStableOffset = -1L;
            }
            if (_version >= 5) {
                this.logStartOffset = _readable.readLong();
            } else {
                this.logStartOffset = -1L;
            }
            {
                this.divergingEpoch = new EpochEndOffset();
            }
            {
                this.currentLeader = new LeaderIdAndEpoch();
            }
            {
                this.snapshotId = new SnapshotId();
            }
            if (_version >= 4) {
                if (_version >= 12) {
                    int arrayLength;
                    arrayLength = _readable.readUnsignedVarint() - 1;
                    if (arrayLength < 0) {
                        this.abortedTransactions = null;
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<AbortedTransaction> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new AbortedTransaction(_readable, _version));
                        }
                        this.abortedTransactions = newCollection;
                    }
                } else {
                    int arrayLength;
                    arrayLength = _readable.readInt();
                    if (arrayLength < 0) {
                        this.abortedTransactions = null;
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<AbortedTransaction> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new AbortedTransaction(_readable, _version));
                        }
                        this.abortedTransactions = newCollection;
                    }
                }
            } else {
                this.abortedTransactions = new ArrayList<AbortedTransaction>(0);
            }
            if (_version >= 11) {
                this.preferredReadReplica = _readable.readInt();
            } else {
                this.preferredReadReplica = -1;
            }
            {
                int length;
                if (_version >= 12) {
                    length = _readable.readUnsignedVarint() - 1;
                } else {
                    length = _readable.readInt();
                }
                if (length < 0) {
                    this.records = null;
                } else {
                    this.records = _readable.readRecords(length);
                }
            }
            this._unknownTaggedFields = null;
            if (_version >= 12) {
                int _numTaggedFields = _readable.readUnsignedVarint();
                for (int _i = 0; _i < _numTaggedFields; _i++) {
                    int _tag = _readable.readUnsignedVarint();
                    int _size = _readable.readUnsignedVarint();
                    switch (_tag) {
                        case 0: {
                            this.divergingEpoch = new EpochEndOffset(_readable, _version);
                            break;
                        }
                        case 1: {
                            this.currentLeader = new LeaderIdAndEpoch(_readable, _version);
                            break;
                        }
                        case 2: {
                            this.snapshotId = new SnapshotId(_readable, _version);
                            break;
                        }
                        default:
                            this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                            break;
                    }
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _writable.writeInt(partitionIndex);
            _writable.writeShort(errorCode);
            _writable.writeLong(highWatermark);
            if (_version >= 4) {
                _writable.writeLong(lastStableOffset);
            }
            if (_version >= 5) {
                _writable.writeLong(logStartOffset);
            }
            if (_version >= 12) {
                if (!this.divergingEpoch.equals(new EpochEndOffset())) {
                    _numTaggedFields++;
                }
            } else {
                if (!this.divergingEpoch.equals(new EpochEndOffset())) {
                    throw new UnsupportedVersionException("Attempted to write a non-default divergingEpoch at version " + _version);
                }
            }
            if (_version >= 12) {
                if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                    _numTaggedFields++;
                }
            } else {
                if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                    throw new UnsupportedVersionException("Attempted to write a non-default currentLeader at version " + _version);
                }
            }
            if (_version >= 12) {
                if (!this.snapshotId.equals(new SnapshotId())) {
                    _numTaggedFields++;
                }
            } else {
                if (!this.snapshotId.equals(new SnapshotId())) {
                    throw new UnsupportedVersionException("Attempted to write a non-default snapshotId at version " + _version);
                }
            }
            if (_version >= 4) {
                if (_version >= 12) {
                    if (abortedTransactions == null) {
                        _writable.writeUnsignedVarint(0);
                    } else {
                        _writable.writeUnsignedVarint(abortedTransactions.size() + 1);
                        for (AbortedTransaction abortedTransactionsElement : abortedTransactions) {
                            abortedTransactionsElement.write(_writable, _cache, _version);
                        }
                    }
                } else {
                    if (abortedTransactions == null) {
                        _writable.writeInt(-1);
                    } else {
                        _writable.writeInt(abortedTransactions.size());
                        for (AbortedTransaction abortedTransactionsElement : abortedTransactions) {
                            abortedTransactionsElement.write(_writable, _cache, _version);
                        }
                    }
                }
            }
            if (_version >= 11) {
                _writable.writeInt(preferredReadReplica);
            } else {
                if (this.preferredReadReplica != -1) {
                    throw new UnsupportedVersionException("Attempted to write a non-default preferredReadReplica at version " + _version);
                }
            }
            if (records == null) {
                if (_version >= 12) {
                    _writable.writeUnsignedVarint(0);
                } else {
                    _writable.writeInt(-1);
                }
            } else {
                if (_version >= 12) {
                    _writable.writeUnsignedVarint(records.sizeInBytes() + 1);
                } else {
                    _writable.writeInt(records.sizeInBytes());
                }
                _writable.writeRecords(records);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 12) {
                _writable.writeUnsignedVarint(_numTaggedFields);
                {
                    if (!this.divergingEpoch.equals(new EpochEndOffset())) {
                        _writable.writeUnsignedVarint(0);
                        _writable.writeUnsignedVarint(this.divergingEpoch.size(_cache, _version));
                        divergingEpoch.write(_writable, _cache, _version);
                    }
                }
                {
                    if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                        _writable.writeUnsignedVarint(1);
                        _writable.writeUnsignedVarint(this.currentLeader.size(_cache, _version));
                        currentLeader.write(_writable, _cache, _version);
                    }
                }
                {
                    if (!this.snapshotId.equals(new SnapshotId())) {
                        _writable.writeUnsignedVarint(2);
                        _writable.writeUnsignedVarint(this.snapshotId.size(_cache, _version));
                        snapshotId.write(_writable, _cache, _version);
                    }
                }
                _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of PartitionData");
            }
            _size.addBytes(4);
            _size.addBytes(2);
            _size.addBytes(8);
            if (_version >= 4) {
                _size.addBytes(8);
            }
            if (_version >= 5) {
                _size.addBytes(8);
            }
            if (_version >= 12) {
                {
                    if (!this.divergingEpoch.equals(new EpochEndOffset())) {
                        _numTaggedFields++;
                        _size.addBytes(1);
                        int _sizeBeforeStruct = _size.totalSize();
                        this.divergingEpoch.addSize(_size, _cache, _version);
                        int _structSize = _size.totalSize() - _sizeBeforeStruct;
                        _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_structSize));
                    }
                }
            }
            if (_version >= 12) {
                {
                    if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                        _numTaggedFields++;
                        _size.addBytes(1);
                        int _sizeBeforeStruct = _size.totalSize();
                        this.currentLeader.addSize(_size, _cache, _version);
                        int _structSize = _size.totalSize() - _sizeBeforeStruct;
                        _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_structSize));
                    }
                }
            }
            if (_version >= 12) {
                {
                    if (!this.snapshotId.equals(new SnapshotId())) {
                        _numTaggedFields++;
                        _size.addBytes(1);
                        int _sizeBeforeStruct = _size.totalSize();
                        this.snapshotId.addSize(_size, _cache, _version);
                        int _structSize = _size.totalSize() - _sizeBeforeStruct;
                        _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_structSize));
                    }
                }
            }
            if (_version >= 4) {
                if (abortedTransactions == null) {
                    if (_version >= 12) {
                        _size.addBytes(1);
                    } else {
                        _size.addBytes(4);
                    }
                } else {
                    if (_version >= 12) {
                        _size.addBytes(ByteUtils.sizeOfUnsignedVarint(abortedTransactions.size() + 1));
                    } else {
                        _size.addBytes(4);
                    }
                    for (AbortedTransaction abortedTransactionsElement : abortedTransactions) {
                        abortedTransactionsElement.addSize(_size, _cache, _version);
                    }
                }
            }
            if (_version >= 11) {
                _size.addBytes(4);
            }
            if (records == null) {
                if (_version >= 12) {
                    _size.addBytes(1);
                } else {
                    _size.addBytes(4);
                }
            } else {
                _size.addZeroCopyBytes(records.sizeInBytes());
                if (_version >= 12) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(records.sizeInBytes() + 1));
                } else {
                    _size.addBytes(4);
                }
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            if (_version >= 12) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PartitionData)) return false;
            PartitionData other = (PartitionData) obj;
            if (partitionIndex != other.partitionIndex) return false;
            if (errorCode != other.errorCode) return false;
            if (highWatermark != other.highWatermark) return false;
            if (lastStableOffset != other.lastStableOffset) return false;
            if (logStartOffset != other.logStartOffset) return false;
            if (this.divergingEpoch == null) {
                if (other.divergingEpoch != null) return false;
            } else {
                if (!this.divergingEpoch.equals(other.divergingEpoch)) return false;
            }
            if (this.currentLeader == null) {
                if (other.currentLeader != null) return false;
            } else {
                if (!this.currentLeader.equals(other.currentLeader)) return false;
            }
            if (this.snapshotId == null) {
                if (other.snapshotId != null) return false;
            } else {
                if (!this.snapshotId.equals(other.snapshotId)) return false;
            }
            if (this.abortedTransactions == null) {
                if (other.abortedTransactions != null) return false;
            } else {
                if (!this.abortedTransactions.equals(other.abortedTransactions)) return false;
            }
            if (preferredReadReplica != other.preferredReadReplica) return false;
            if (!Objects.equals(this.records, other.records)) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + partitionIndex;
            hashCode = 31 * hashCode + errorCode;
            hashCode = 31 * hashCode + ((int) (highWatermark >> 32) ^ (int) highWatermark);
            hashCode = 31 * hashCode + ((int) (lastStableOffset >> 32) ^ (int) lastStableOffset);
            hashCode = 31 * hashCode + ((int) (logStartOffset >> 32) ^ (int) logStartOffset);
            hashCode = 31 * hashCode + (divergingEpoch == null ? 0 : divergingEpoch.hashCode());
            hashCode = 31 * hashCode + (currentLeader == null ? 0 : currentLeader.hashCode());
            hashCode = 31 * hashCode + (snapshotId == null ? 0 : snapshotId.hashCode());
            hashCode = 31 * hashCode + (abortedTransactions == null ? 0 : abortedTransactions.hashCode());
            hashCode = 31 * hashCode + preferredReadReplica;
            hashCode = 31 * hashCode + Objects.hashCode(records);
            return hashCode;
        }
        
        @Override
        public PartitionData duplicate() {
            PartitionData _duplicate = new PartitionData();
            _duplicate.partitionIndex = partitionIndex;
            _duplicate.errorCode = errorCode;
            _duplicate.highWatermark = highWatermark;
            _duplicate.lastStableOffset = lastStableOffset;
            _duplicate.logStartOffset = logStartOffset;
            _duplicate.divergingEpoch = divergingEpoch.duplicate();
            _duplicate.currentLeader = currentLeader.duplicate();
            _duplicate.snapshotId = snapshotId.duplicate();
            if (abortedTransactions == null) {
                _duplicate.abortedTransactions = null;
            } else {
                ArrayList<AbortedTransaction> newAbortedTransactions = new ArrayList<AbortedTransaction>(abortedTransactions.size());
                for (AbortedTransaction _element : abortedTransactions) {
                    newAbortedTransactions.add(_element.duplicate());
                }
                _duplicate.abortedTransactions = newAbortedTransactions;
            }
            _duplicate.preferredReadReplica = preferredReadReplica;
            if (records == null) {
                _duplicate.records = null;
            } else {
                _duplicate.records = MemoryRecords.readableRecords(((MemoryRecords) records).buffer().duplicate());
            }
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "PartitionData("
                + "partitionIndex=" + partitionIndex
                + ", errorCode=" + errorCode
                + ", highWatermark=" + highWatermark
                + ", lastStableOffset=" + lastStableOffset
                + ", logStartOffset=" + logStartOffset
                + ", divergingEpoch=" + divergingEpoch.toString()
                + ", currentLeader=" + currentLeader.toString()
                + ", snapshotId=" + snapshotId.toString()
                + ", abortedTransactions=" + ((abortedTransactions == null) ? "null" : MessageUtil.deepToString(abortedTransactions.iterator()))
                + ", preferredReadReplica=" + preferredReadReplica
                + ", records=" + records
                + ")";
        }
        
        public int partitionIndex() {
            return this.partitionIndex;
        }
        
        public short errorCode() {
            return this.errorCode;
        }
        
        public long highWatermark() {
            return this.highWatermark;
        }
        
        public long lastStableOffset() {
            return this.lastStableOffset;
        }
        
        public long logStartOffset() {
            return this.logStartOffset;
        }
        
        public EpochEndOffset divergingEpoch() {
            return this.divergingEpoch;
        }
        
        public LeaderIdAndEpoch currentLeader() {
            return this.currentLeader;
        }
        
        public SnapshotId snapshotId() {
            return this.snapshotId;
        }
        
        public List<AbortedTransaction> abortedTransactions() {
            return this.abortedTransactions;
        }
        
        public int preferredReadReplica() {
            return this.preferredReadReplica;
        }
        
        public BaseRecords records() {
            return this.records;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public PartitionData setPartitionIndex(int v) {
            this.partitionIndex = v;
            return this;
        }
        
        public PartitionData setErrorCode(short v) {
            this.errorCode = v;
            return this;
        }
        
        public PartitionData setHighWatermark(long v) {
            this.highWatermark = v;
            return this;
        }
        
        public PartitionData setLastStableOffset(long v) {
            this.lastStableOffset = v;
            return this;
        }
        
        public PartitionData setLogStartOffset(long v) {
            this.logStartOffset = v;
            return this;
        }
        
        public PartitionData setDivergingEpoch(EpochEndOffset v) {
            this.divergingEpoch = v;
            return this;
        }
        
        public PartitionData setCurrentLeader(LeaderIdAndEpoch v) {
            this.currentLeader = v;
            return this;
        }
        
        public PartitionData setSnapshotId(SnapshotId v) {
            this.snapshotId = v;
            return this;
        }
        
        public PartitionData setAbortedTransactions(List<AbortedTransaction> v) {
            this.abortedTransactions = v;
            return this;
        }
        
        public PartitionData setPreferredReadReplica(int v) {
            this.preferredReadReplica = v;
            return this;
        }
        
        public PartitionData setRecords(BaseRecords v) {
            this.records = v;
            return this;
        }
    }
    
    public static class EpochEndOffset implements Message {
        int epoch;
        long endOffset;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("epoch", Type.INT32, ""),
                new Field("end_offset", Type.INT64, ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_13 = SCHEMA_12;
        
        public static final Schema SCHEMA_14 = SCHEMA_13;
        
        public static final Schema SCHEMA_15 = SCHEMA_14;
        
        public static final Schema SCHEMA_16 = SCHEMA_15;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            SCHEMA_12,
            SCHEMA_13,
            SCHEMA_14,
            SCHEMA_15,
            SCHEMA_16
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 12;
        public static final short HIGHEST_SUPPORTED_VERSION = 16;
        
        public EpochEndOffset(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public EpochEndOffset() {
            this.epoch = -1;
            this.endOffset = -1L;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 16;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of EpochEndOffset");
            }
            this.epoch = _readable.readInt();
            this.endOffset = _readable.readLong();
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 12) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of EpochEndOffset");
            }
            int _numTaggedFields = 0;
            _writable.writeInt(epoch);
            _writable.writeLong(endOffset);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of EpochEndOffset");
            }
            _size.addBytes(4);
            _size.addBytes(8);
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof EpochEndOffset)) return false;
            EpochEndOffset other = (EpochEndOffset) obj;
            if (epoch != other.epoch) return false;
            if (endOffset != other.endOffset) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + epoch;
            hashCode = 31 * hashCode + ((int) (endOffset >> 32) ^ (int) endOffset);
            return hashCode;
        }
        
        @Override
        public EpochEndOffset duplicate() {
            EpochEndOffset _duplicate = new EpochEndOffset();
            _duplicate.epoch = epoch;
            _duplicate.endOffset = endOffset;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "EpochEndOffset("
                + "epoch=" + epoch
                + ", endOffset=" + endOffset
                + ")";
        }
        
        public int epoch() {
            return this.epoch;
        }
        
        public long endOffset() {
            return this.endOffset;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public EpochEndOffset setEpoch(int v) {
            this.epoch = v;
            return this;
        }
        
        public EpochEndOffset setEndOffset(long v) {
            this.endOffset = v;
            return this;
        }
    }
    
    public static class LeaderIdAndEpoch implements Message {
        int leaderId;
        int leaderEpoch;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("leader_id", Type.INT32, "The ID of the current leader or -1 if the leader is unknown."),
                new Field("leader_epoch", Type.INT32, "The latest known leader epoch"),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_13 = SCHEMA_12;
        
        public static final Schema SCHEMA_14 = SCHEMA_13;
        
        public static final Schema SCHEMA_15 = SCHEMA_14;
        
        public static final Schema SCHEMA_16 = SCHEMA_15;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            SCHEMA_12,
            SCHEMA_13,
            SCHEMA_14,
            SCHEMA_15,
            SCHEMA_16
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 12;
        public static final short HIGHEST_SUPPORTED_VERSION = 16;
        
        public LeaderIdAndEpoch(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public LeaderIdAndEpoch() {
            this.leaderId = -1;
            this.leaderEpoch = -1;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 16;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of LeaderIdAndEpoch");
            }
            this.leaderId = _readable.readInt();
            this.leaderEpoch = _readable.readInt();
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 12) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of LeaderIdAndEpoch");
            }
            int _numTaggedFields = 0;
            _writable.writeInt(leaderId);
            _writable.writeInt(leaderEpoch);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of LeaderIdAndEpoch");
            }
            _size.addBytes(4);
            _size.addBytes(4);
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LeaderIdAndEpoch)) return false;
            LeaderIdAndEpoch other = (LeaderIdAndEpoch) obj;
            if (leaderId != other.leaderId) return false;
            if (leaderEpoch != other.leaderEpoch) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + leaderId;
            hashCode = 31 * hashCode + leaderEpoch;
            return hashCode;
        }
        
        @Override
        public LeaderIdAndEpoch duplicate() {
            LeaderIdAndEpoch _duplicate = new LeaderIdAndEpoch();
            _duplicate.leaderId = leaderId;
            _duplicate.leaderEpoch = leaderEpoch;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "LeaderIdAndEpoch("
                + "leaderId=" + leaderId
                + ", leaderEpoch=" + leaderEpoch
                + ")";
        }
        
        public int leaderId() {
            return this.leaderId;
        }
        
        public int leaderEpoch() {
            return this.leaderEpoch;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public LeaderIdAndEpoch setLeaderId(int v) {
            this.leaderId = v;
            return this;
        }
        
        public LeaderIdAndEpoch setLeaderEpoch(int v) {
            this.leaderEpoch = v;
            return this;
        }
    }
    
    public static class SnapshotId implements Message {
        long endOffset;
        int epoch;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("end_offset", Type.INT64, ""),
                new Field("epoch", Type.INT32, ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_13 = SCHEMA_12;
        
        public static final Schema SCHEMA_14 = SCHEMA_13;
        
        public static final Schema SCHEMA_15 = SCHEMA_14;
        
        public static final Schema SCHEMA_16 = SCHEMA_15;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            SCHEMA_12,
            SCHEMA_13,
            SCHEMA_14,
            SCHEMA_15,
            SCHEMA_16
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 12;
        public static final short HIGHEST_SUPPORTED_VERSION = 16;
        
        public SnapshotId(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public SnapshotId() {
            this.endOffset = -1L;
            this.epoch = -1;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 16;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of SnapshotId");
            }
            this.endOffset = _readable.readLong();
            this.epoch = _readable.readInt();
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 12) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of SnapshotId");
            }
            int _numTaggedFields = 0;
            _writable.writeLong(endOffset);
            _writable.writeInt(epoch);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of SnapshotId");
            }
            _size.addBytes(8);
            _size.addBytes(4);
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SnapshotId)) return false;
            SnapshotId other = (SnapshotId) obj;
            if (endOffset != other.endOffset) return false;
            if (epoch != other.epoch) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + ((int) (endOffset >> 32) ^ (int) endOffset);
            hashCode = 31 * hashCode + epoch;
            return hashCode;
        }
        
        @Override
        public SnapshotId duplicate() {
            SnapshotId _duplicate = new SnapshotId();
            _duplicate.endOffset = endOffset;
            _duplicate.epoch = epoch;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "SnapshotId("
                + "endOffset=" + endOffset
                + ", epoch=" + epoch
                + ")";
        }
        
        public long endOffset() {
            return this.endOffset;
        }
        
        public int epoch() {
            return this.epoch;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public SnapshotId setEndOffset(long v) {
            this.endOffset = v;
            return this;
        }
        
        public SnapshotId setEpoch(int v) {
            this.epoch = v;
            return this;
        }
    }
    
    public static class AbortedTransaction implements Message {
        long producerId;
        long firstOffset;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_4 =
            new Schema(
                new Field("producer_id", Type.INT64, "The producer id associated with the aborted transaction."),
                new Field("first_offset", Type.INT64, "The first offset in the aborted transaction.")
            );
        
        public static final Schema SCHEMA_5 = SCHEMA_4;
        
        public static final Schema SCHEMA_6 = SCHEMA_5;
        
        public static final Schema SCHEMA_7 = SCHEMA_6;
        
        public static final Schema SCHEMA_8 = SCHEMA_7;
        
        public static final Schema SCHEMA_9 = SCHEMA_8;
        
        public static final Schema SCHEMA_10 = SCHEMA_9;
        
        public static final Schema SCHEMA_11 = SCHEMA_10;
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("producer_id", Type.INT64, "The producer id associated with the aborted transaction."),
                new Field("first_offset", Type.INT64, "The first offset in the aborted transaction."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_13 = SCHEMA_12;
        
        public static final Schema SCHEMA_14 = SCHEMA_13;
        
        public static final Schema SCHEMA_15 = SCHEMA_14;
        
        public static final Schema SCHEMA_16 = SCHEMA_15;
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            null,
            null,
            SCHEMA_4,
            SCHEMA_5,
            SCHEMA_6,
            SCHEMA_7,
            SCHEMA_8,
            SCHEMA_9,
            SCHEMA_10,
            SCHEMA_11,
            SCHEMA_12,
            SCHEMA_13,
            SCHEMA_14,
            SCHEMA_15,
            SCHEMA_16
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 4;
        public static final short HIGHEST_SUPPORTED_VERSION = 16;
        
        public AbortedTransaction(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public AbortedTransaction() {
            this.producerId = 0L;
            this.firstOffset = 0L;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 16;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of AbortedTransaction");
            }
            this.producerId = _readable.readLong();
            this.firstOffset = _readable.readLong();
            this._unknownTaggedFields = null;
            if (_version >= 12) {
                int _numTaggedFields = _readable.readUnsignedVarint();
                for (int _i = 0; _i < _numTaggedFields; _i++) {
                    int _tag = _readable.readUnsignedVarint();
                    int _size = _readable.readUnsignedVarint();
                    switch (_tag) {
                        default:
                            this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                            break;
                    }
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 4) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of AbortedTransaction");
            }
            int _numTaggedFields = 0;
            _writable.writeLong(producerId);
            _writable.writeLong(firstOffset);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 12) {
                _writable.writeUnsignedVarint(_numTaggedFields);
                _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of AbortedTransaction");
            }
            _size.addBytes(8);
            _size.addBytes(8);
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            if (_version >= 12) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof AbortedTransaction)) return false;
            AbortedTransaction other = (AbortedTransaction) obj;
            if (producerId != other.producerId) return false;
            if (firstOffset != other.firstOffset) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + ((int) (producerId >> 32) ^ (int) producerId);
            hashCode = 31 * hashCode + ((int) (firstOffset >> 32) ^ (int) firstOffset);
            return hashCode;
        }
        
        @Override
        public AbortedTransaction duplicate() {
            AbortedTransaction _duplicate = new AbortedTransaction();
            _duplicate.producerId = producerId;
            _duplicate.firstOffset = firstOffset;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "AbortedTransaction("
                + "producerId=" + producerId
                + ", firstOffset=" + firstOffset
                + ")";
        }
        
        public long producerId() {
            return this.producerId;
        }
        
        public long firstOffset() {
            return this.firstOffset;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public AbortedTransaction setProducerId(long v) {
            this.producerId = v;
            return this;
        }
        
        public AbortedTransaction setFirstOffset(long v) {
            this.firstOffset = v;
            return this;
        }
    }
    
    public static class NodeEndpoint implements Message, ImplicitLinkedHashMultiCollection.Element {
        int nodeId;
        String host;
        int port;
        String rack;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_16 =
            new Schema(
                new Field("node_id", Type.INT32, "The ID of the associated node."),
                new Field("host", Type.COMPACT_STRING, "The node's hostname."),
                new Field("port", Type.INT32, "The node's port."),
                new Field("rack", Type.COMPACT_NULLABLE_STRING, "The rack of the node, or null if it has not been assigned to a rack."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            SCHEMA_16
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 16;
        public static final short HIGHEST_SUPPORTED_VERSION = 16;
        
        public NodeEndpoint(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public NodeEndpoint() {
            this.nodeId = 0;
            this.host = "";
            this.port = 0;
            this.rack = null;
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 16;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of NodeEndpoint");
            }
            this.nodeId = _readable.readInt();
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field host was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field host had invalid length " + length);
                } else {
                    this.host = _readable.readString(length);
                }
            }
            this.port = _readable.readInt();
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    this.rack = null;
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field rack had invalid length " + length);
                } else {
                    this.rack = _readable.readString(length);
                }
            }
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 16) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of NodeEndpoint");
            }
            int _numTaggedFields = 0;
            _writable.writeInt(nodeId);
            {
                byte[] _stringBytes = _cache.getSerializedValue(host);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeInt(port);
            if (rack == null) {
                _writable.writeUnsignedVarint(0);
            } else {
                byte[] _stringBytes = _cache.getSerializedValue(rack);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 16) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of NodeEndpoint");
            }
            _size.addBytes(4);
            {
                byte[] _stringBytes = host.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'host' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(host, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            _size.addBytes(4);
            if (rack == null) {
                _size.addBytes(1);
            } else {
                byte[] _stringBytes = rack.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'rack' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(rack, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean elementKeysAreEqual(Object obj) {
            if (!(obj instanceof NodeEndpoint)) return false;
            NodeEndpoint other = (NodeEndpoint) obj;
            if (nodeId != other.nodeId) return false;
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof NodeEndpoint)) return false;
            NodeEndpoint other = (NodeEndpoint) obj;
            if (nodeId != other.nodeId) return false;
            if (this.host == null) {
                if (other.host != null) return false;
            } else {
                if (!this.host.equals(other.host)) return false;
            }
            if (port != other.port) return false;
            if (this.rack == null) {
                if (other.rack != null) return false;
            } else {
                if (!this.rack.equals(other.rack)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + nodeId;
            return hashCode;
        }
        
        @Override
        public NodeEndpoint duplicate() {
            NodeEndpoint _duplicate = new NodeEndpoint();
            _duplicate.nodeId = nodeId;
            _duplicate.host = host;
            _duplicate.port = port;
            if (rack == null) {
                _duplicate.rack = null;
            } else {
                _duplicate.rack = rack;
            }
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "NodeEndpoint("
                + "nodeId=" + nodeId
                + ", host=" + ((host == null) ? "null" : "'" + host.toString() + "'")
                + ", port=" + port
                + ", rack=" + ((rack == null) ? "null" : "'" + rack.toString() + "'")
                + ")";
        }
        
        public int nodeId() {
            return this.nodeId;
        }
        
        public String host() {
            return this.host;
        }
        
        public int port() {
            return this.port;
        }
        
        public String rack() {
            return this.rack;
        }
        
        @Override
        public int next() {
            return this.next;
        }
        
        @Override
        public int prev() {
            return this.prev;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public NodeEndpoint setNodeId(int v) {
            this.nodeId = v;
            return this;
        }
        
        public NodeEndpoint setHost(String v) {
            this.host = v;
            return this;
        }
        
        public NodeEndpoint setPort(int v) {
            this.port = v;
            return this;
        }
        
        public NodeEndpoint setRack(String v) {
            this.rack = v;
            return this;
        }
        
        @Override
        public void setNext(int v) {
            this.next = v;
        }
        
        @Override
        public void setPrev(int v) {
            this.prev = v;
        }
    }
    
    public static class NodeEndpointCollection extends ImplicitLinkedHashMultiCollection<NodeEndpoint> {
        public NodeEndpointCollection() {
            super();
        }
        
        public NodeEndpointCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public NodeEndpointCollection(Iterator<NodeEndpoint> iterator) {
            super(iterator);
        }
        
        public NodeEndpoint find(int nodeId) {
            NodeEndpoint _key = new NodeEndpoint();
            _key.setNodeId(nodeId);
            return find(_key);
        }
        
        public List<NodeEndpoint> findAll(int nodeId) {
            NodeEndpoint _key = new NodeEndpoint();
            _key.setNodeId(nodeId);
            return findAll(_key);
        }
        
        public NodeEndpointCollection duplicate() {
            NodeEndpointCollection _duplicate = new NodeEndpointCollection(size());
            for (NodeEndpoint _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
}
