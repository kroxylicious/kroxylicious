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


public class ProduceRequestData implements ApiMessage {
    String transactionalId;
    short acks;
    int timeoutMs;
    TopicProduceDataCollection topicData;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("acks", Type.INT16, "The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR."),
            new Field("timeout_ms", Type.INT32, "The timeout to await a response in milliseconds."),
            new Field("topic_data", new ArrayOf(TopicProduceData.SCHEMA_0), "Each topic to produce to.")
        );
    
    public static final Schema SCHEMA_1 = SCHEMA_0;
    
    public static final Schema SCHEMA_2 = SCHEMA_1;
    
    public static final Schema SCHEMA_3 =
        new Schema(
            new Field("transactional_id", Type.NULLABLE_STRING, "The transactional ID, or null if the producer is not transactional."),
            new Field("acks", Type.INT16, "The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR."),
            new Field("timeout_ms", Type.INT32, "The timeout to await a response in milliseconds."),
            new Field("topic_data", new ArrayOf(TopicProduceData.SCHEMA_0), "Each topic to produce to.")
        );
    
    public static final Schema SCHEMA_4 = SCHEMA_3;
    
    public static final Schema SCHEMA_5 = SCHEMA_4;
    
    public static final Schema SCHEMA_6 = SCHEMA_5;
    
    public static final Schema SCHEMA_7 = SCHEMA_6;
    
    public static final Schema SCHEMA_8 = SCHEMA_7;
    
    public static final Schema SCHEMA_9 =
        new Schema(
            new Field("transactional_id", Type.COMPACT_NULLABLE_STRING, "The transactional ID, or null if the producer is not transactional."),
            new Field("acks", Type.INT16, "The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR."),
            new Field("timeout_ms", Type.INT32, "The timeout to await a response in milliseconds."),
            new Field("topic_data", new CompactArrayOf(TopicProduceData.SCHEMA_9), "Each topic to produce to."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_10 = SCHEMA_9;
    
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
        SCHEMA_10
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 10;
    
    public ProduceRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public ProduceRequestData() {
        this.transactionalId = null;
        this.acks = (short) 0;
        this.timeoutMs = 0;
        this.topicData = new TopicProduceDataCollection(0);
    }
    
    @Override
    public short apiKey() {
        return 0;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 10;
    }
    
    @Override
    public final void read(Readable _readable, short _version) {
        if (_version >= 3) {
            int length;
            if (_version >= 9) {
                length = _readable.readUnsignedVarint() - 1;
            } else {
                length = _readable.readShort();
            }
            if (length < 0) {
                this.transactionalId = null;
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field transactionalId had invalid length " + length);
            } else {
                this.transactionalId = _readable.readString(length);
            }
        } else {
            this.transactionalId = null;
        }
        this.acks = _readable.readShort();
        this.timeoutMs = _readable.readInt();
        {
            if (_version >= 9) {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field topicData was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    TopicProduceDataCollection newCollection = new TopicProduceDataCollection(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new TopicProduceData(_readable, _version));
                    }
                    this.topicData = newCollection;
                }
            } else {
                int arrayLength;
                arrayLength = _readable.readInt();
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field topicData was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    TopicProduceDataCollection newCollection = new TopicProduceDataCollection(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new TopicProduceData(_readable, _version));
                    }
                    this.topicData = newCollection;
                }
            }
        }
        this._unknownTaggedFields = null;
        if (_version >= 9) {
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
        if (_version >= 3) {
            if (transactionalId == null) {
                if (_version >= 9) {
                    _writable.writeUnsignedVarint(0);
                } else {
                    _writable.writeShort((short) -1);
                }
            } else {
                byte[] _stringBytes = _cache.getSerializedValue(transactionalId);
                if (_version >= 9) {
                    _writable.writeUnsignedVarint(_stringBytes.length + 1);
                } else {
                    _writable.writeShort((short) _stringBytes.length);
                }
                _writable.writeByteArray(_stringBytes);
            }
        } else {
            if (this.transactionalId != null) {
                throw new UnsupportedVersionException("Attempted to write a non-default transactionalId at version " + _version);
            }
        }
        _writable.writeShort(acks);
        _writable.writeInt(timeoutMs);
        if (_version >= 9) {
            _writable.writeUnsignedVarint(topicData.size() + 1);
            for (TopicProduceData topicDataElement : topicData) {
                topicDataElement.write(_writable, _cache, _version);
            }
        } else {
            _writable.writeInt(topicData.size());
            for (TopicProduceData topicDataElement : topicData) {
                topicDataElement.write(_writable, _cache, _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 9) {
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
        if (_version >= 3) {
            if (transactionalId == null) {
                if (_version >= 9) {
                    _size.addBytes(1);
                } else {
                    _size.addBytes(2);
                }
            } else {
                byte[] _stringBytes = transactionalId.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'transactionalId' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(transactionalId, _stringBytes);
                if (_version >= 9) {
                    _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
                } else {
                    _size.addBytes(_stringBytes.length + 2);
                }
            }
        }
        _size.addBytes(2);
        _size.addBytes(4);
        {
            if (_version >= 9) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(topicData.size() + 1));
            } else {
                _size.addBytes(4);
            }
            for (TopicProduceData topicDataElement : topicData) {
                topicDataElement.addSize(_size, _cache, _version);
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
        if (_version >= 9) {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ProduceRequestData)) return false;
        ProduceRequestData other = (ProduceRequestData) obj;
        if (this.transactionalId == null) {
            if (other.transactionalId != null) return false;
        } else {
            if (!this.transactionalId.equals(other.transactionalId)) return false;
        }
        if (acks != other.acks) return false;
        if (timeoutMs != other.timeoutMs) return false;
        if (this.topicData == null) {
            if (other.topicData != null) return false;
        } else {
            if (!this.topicData.equals(other.topicData)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (transactionalId == null ? 0 : transactionalId.hashCode());
        hashCode = 31 * hashCode + acks;
        hashCode = 31 * hashCode + timeoutMs;
        hashCode = 31 * hashCode + (topicData == null ? 0 : topicData.hashCode());
        return hashCode;
    }
    
    @Override
    public ProduceRequestData duplicate() {
        ProduceRequestData _duplicate = new ProduceRequestData();
        if (transactionalId == null) {
            _duplicate.transactionalId = null;
        } else {
            _duplicate.transactionalId = transactionalId;
        }
        _duplicate.acks = acks;
        _duplicate.timeoutMs = timeoutMs;
        TopicProduceDataCollection newTopicData = new TopicProduceDataCollection(topicData.size());
        for (TopicProduceData _element : topicData) {
            newTopicData.add(_element.duplicate());
        }
        _duplicate.topicData = newTopicData;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "ProduceRequestData("
            + "transactionalId=" + ((transactionalId == null) ? "null" : "'" + transactionalId.toString() + "'")
            + ", acks=" + acks
            + ", timeoutMs=" + timeoutMs
            + ", topicData=" + MessageUtil.deepToString(topicData.iterator())
            + ")";
    }
    
    public String transactionalId() {
        return this.transactionalId;
    }
    
    public short acks() {
        return this.acks;
    }
    
    public int timeoutMs() {
        return this.timeoutMs;
    }
    
    public TopicProduceDataCollection topicData() {
        return this.topicData;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public ProduceRequestData setTransactionalId(String v) {
        this.transactionalId = v;
        return this;
    }
    
    public ProduceRequestData setAcks(short v) {
        this.acks = v;
        return this;
    }
    
    public ProduceRequestData setTimeoutMs(int v) {
        this.timeoutMs = v;
        return this;
    }
    
    public ProduceRequestData setTopicData(TopicProduceDataCollection v) {
        this.topicData = v;
        return this;
    }
    
    public static class TopicProduceData implements Message, ImplicitLinkedHashMultiCollection.Element {
        String name;
        List<PartitionProduceData> partitionData;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("name", Type.STRING, "The topic name."),
                new Field("partition_data", new ArrayOf(PartitionProduceData.SCHEMA_0), "Each partition to produce to.")
            );
        
        public static final Schema SCHEMA_1 = SCHEMA_0;
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema SCHEMA_3 = SCHEMA_2;
        
        public static final Schema SCHEMA_4 = SCHEMA_3;
        
        public static final Schema SCHEMA_5 = SCHEMA_4;
        
        public static final Schema SCHEMA_6 = SCHEMA_5;
        
        public static final Schema SCHEMA_7 = SCHEMA_6;
        
        public static final Schema SCHEMA_8 = SCHEMA_7;
        
        public static final Schema SCHEMA_9 =
            new Schema(
                new Field("name", Type.COMPACT_STRING, "The topic name."),
                new Field("partition_data", new CompactArrayOf(PartitionProduceData.SCHEMA_9), "Each partition to produce to."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_10 = SCHEMA_9;
        
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
            SCHEMA_10
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 10;
        
        public TopicProduceData(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public TopicProduceData() {
            this.name = "";
            this.partitionData = new ArrayList<PartitionProduceData>(0);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 10;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 10) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of TopicProduceData");
            }
            {
                int length;
                if (_version >= 9) {
                    length = _readable.readUnsignedVarint() - 1;
                } else {
                    length = _readable.readShort();
                }
                if (length < 0) {
                    throw new RuntimeException("non-nullable field name was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field name had invalid length " + length);
                } else {
                    this.name = _readable.readString(length);
                }
            }
            {
                if (_version >= 9) {
                    int arrayLength;
                    arrayLength = _readable.readUnsignedVarint() - 1;
                    if (arrayLength < 0) {
                        throw new RuntimeException("non-nullable field partitionData was serialized as null");
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<PartitionProduceData> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new PartitionProduceData(_readable, _version));
                        }
                        this.partitionData = newCollection;
                    }
                } else {
                    int arrayLength;
                    arrayLength = _readable.readInt();
                    if (arrayLength < 0) {
                        throw new RuntimeException("non-nullable field partitionData was serialized as null");
                    } else {
                        if (arrayLength > _readable.remaining()) {
                            throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                        }
                        ArrayList<PartitionProduceData> newCollection = new ArrayList<>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new PartitionProduceData(_readable, _version));
                        }
                        this.partitionData = newCollection;
                    }
                }
            }
            this._unknownTaggedFields = null;
            if (_version >= 9) {
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
            {
                byte[] _stringBytes = _cache.getSerializedValue(name);
                if (_version >= 9) {
                    _writable.writeUnsignedVarint(_stringBytes.length + 1);
                } else {
                    _writable.writeShort((short) _stringBytes.length);
                }
                _writable.writeByteArray(_stringBytes);
            }
            if (_version >= 9) {
                _writable.writeUnsignedVarint(partitionData.size() + 1);
                for (PartitionProduceData partitionDataElement : partitionData) {
                    partitionDataElement.write(_writable, _cache, _version);
                }
            } else {
                _writable.writeInt(partitionData.size());
                for (PartitionProduceData partitionDataElement : partitionData) {
                    partitionDataElement.write(_writable, _cache, _version);
                }
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 9) {
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
            if (_version > 10) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of TopicProduceData");
            }
            {
                byte[] _stringBytes = name.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'name' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(name, _stringBytes);
                if (_version >= 9) {
                    _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
                } else {
                    _size.addBytes(_stringBytes.length + 2);
                }
            }
            {
                if (_version >= 9) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(partitionData.size() + 1));
                } else {
                    _size.addBytes(4);
                }
                for (PartitionProduceData partitionDataElement : partitionData) {
                    partitionDataElement.addSize(_size, _cache, _version);
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
            if (_version >= 9) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public boolean elementKeysAreEqual(Object obj) {
            if (!(obj instanceof TopicProduceData)) return false;
            TopicProduceData other = (TopicProduceData) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TopicProduceData)) return false;
            TopicProduceData other = (TopicProduceData) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            if (this.partitionData == null) {
                if (other.partitionData != null) return false;
            } else {
                if (!this.partitionData.equals(other.partitionData)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
            return hashCode;
        }
        
        @Override
        public TopicProduceData duplicate() {
            TopicProduceData _duplicate = new TopicProduceData();
            _duplicate.name = name;
            ArrayList<PartitionProduceData> newPartitionData = new ArrayList<PartitionProduceData>(partitionData.size());
            for (PartitionProduceData _element : partitionData) {
                newPartitionData.add(_element.duplicate());
            }
            _duplicate.partitionData = newPartitionData;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "TopicProduceData("
                + "name=" + ((name == null) ? "null" : "'" + name.toString() + "'")
                + ", partitionData=" + MessageUtil.deepToString(partitionData.iterator())
                + ")";
        }
        
        public String name() {
            return this.name;
        }
        
        public List<PartitionProduceData> partitionData() {
            return this.partitionData;
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
        
        public TopicProduceData setName(String v) {
            this.name = v;
            return this;
        }
        
        public TopicProduceData setPartitionData(List<PartitionProduceData> v) {
            this.partitionData = v;
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
    
    public static class PartitionProduceData implements Message {
        int index;
        BaseRecords records;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("index", Type.INT32, "The partition index."),
                new Field("records", Type.RECORDS, "The record data to be produced.")
            );
        
        public static final Schema SCHEMA_1 = SCHEMA_0;
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema SCHEMA_3 = SCHEMA_2;
        
        public static final Schema SCHEMA_4 = SCHEMA_3;
        
        public static final Schema SCHEMA_5 = SCHEMA_4;
        
        public static final Schema SCHEMA_6 = SCHEMA_5;
        
        public static final Schema SCHEMA_7 = SCHEMA_6;
        
        public static final Schema SCHEMA_8 = SCHEMA_7;
        
        public static final Schema SCHEMA_9 =
            new Schema(
                new Field("index", Type.INT32, "The partition index."),
                new Field("records", Type.COMPACT_RECORDS, "The record data to be produced."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema SCHEMA_10 = SCHEMA_9;
        
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
            SCHEMA_10
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 10;
        
        public PartitionProduceData(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public PartitionProduceData() {
            this.index = 0;
            this.records = null;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 10;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 10) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of PartitionProduceData");
            }
            this.index = _readable.readInt();
            {
                int length;
                if (_version >= 9) {
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
            if (_version >= 9) {
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
            _writable.writeInt(index);
            if (records == null) {
                if (_version >= 9) {
                    _writable.writeUnsignedVarint(0);
                } else {
                    _writable.writeInt(-1);
                }
            } else {
                if (_version >= 9) {
                    _writable.writeUnsignedVarint(records.sizeInBytes() + 1);
                } else {
                    _writable.writeInt(records.sizeInBytes());
                }
                _writable.writeRecords(records);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 9) {
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
            if (_version > 10) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of PartitionProduceData");
            }
            _size.addBytes(4);
            if (records == null) {
                if (_version >= 9) {
                    _size.addBytes(1);
                } else {
                    _size.addBytes(4);
                }
            } else {
                _size.addZeroCopyBytes(records.sizeInBytes());
                if (_version >= 9) {
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
            if (_version >= 9) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PartitionProduceData)) return false;
            PartitionProduceData other = (PartitionProduceData) obj;
            if (index != other.index) return false;
            if (!Objects.equals(this.records, other.records)) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + index;
            hashCode = 31 * hashCode + Objects.hashCode(records);
            return hashCode;
        }
        
        @Override
        public PartitionProduceData duplicate() {
            PartitionProduceData _duplicate = new PartitionProduceData();
            _duplicate.index = index;
            if (records == null) {
                _duplicate.records = null;
            } else {
                _duplicate.records = MemoryRecords.readableRecords(((MemoryRecords) records).buffer().duplicate());
            }
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "PartitionProduceData("
                + "index=" + index
                + ", records=" + records
                + ")";
        }
        
        public int index() {
            return this.index;
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
        
        public PartitionProduceData setIndex(int v) {
            this.index = v;
            return this;
        }
        
        public PartitionProduceData setRecords(BaseRecords v) {
            this.records = v;
            return this;
        }
    }
    
    public static class TopicProduceDataCollection extends ImplicitLinkedHashMultiCollection<TopicProduceData> {
        public TopicProduceDataCollection() {
            super();
        }
        
        public TopicProduceDataCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public TopicProduceDataCollection(Iterator<TopicProduceData> iterator) {
            super(iterator);
        }
        
        public TopicProduceData find(String name) {
            TopicProduceData _key = new TopicProduceData();
            _key.setName(name);
            return find(_key);
        }
        
        public List<TopicProduceData> findAll(String name) {
            TopicProduceData _key = new TopicProduceData();
            _key.setName(name);
            return findAll(_key);
        }
        
        public TopicProduceDataCollection duplicate() {
            TopicProduceDataCollection _duplicate = new TopicProduceDataCollection(size());
            for (TopicProduceData _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
}
