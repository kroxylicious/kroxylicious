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
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ByteUtils;

import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class FetchSnapshotResponseData implements ApiMessage {
    int throttleTimeMs;
    short errorCode;
    List<TopicSnapshot> topics;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The top level response error code."),
            new Field("topics", new CompactArrayOf(TopicSnapshot.SCHEMA_0), "The topics to fetch."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public FetchSnapshotResponseData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public FetchSnapshotResponseData() {
        this.throttleTimeMs = 0;
        this.errorCode = (short) 0;
        this.topics = new ArrayList<TopicSnapshot>(0);
    }
    
    @Override
    public short apiKey() {
        return 59;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 0;
    }
    
    @Override
    public final void read(Readable _readable, short _version) {
        this.throttleTimeMs = _readable.readInt();
        this.errorCode = _readable.readShort();
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field topics was serialized as null");
            } else {
                if (arrayLength > _readable.remaining()) {
                    throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                }
                ArrayList<TopicSnapshot> newCollection = new ArrayList<>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new TopicSnapshot(_readable, _version));
                }
                this.topics = newCollection;
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
        int _numTaggedFields = 0;
        _writable.writeInt(throttleTimeMs);
        _writable.writeShort(errorCode);
        _writable.writeUnsignedVarint(topics.size() + 1);
        for (TopicSnapshot topicsElement : topics) {
            topicsElement.write(_writable, _cache, _version);
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(4);
        _size.addBytes(2);
        {
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(topics.size() + 1));
            for (TopicSnapshot topicsElement : topics) {
                topicsElement.addSize(_size, _cache, _version);
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
        _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FetchSnapshotResponseData)) return false;
        FetchSnapshotResponseData other = (FetchSnapshotResponseData) obj;
        if (throttleTimeMs != other.throttleTimeMs) return false;
        if (errorCode != other.errorCode) return false;
        if (this.topics == null) {
            if (other.topics != null) return false;
        } else {
            if (!this.topics.equals(other.topics)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + throttleTimeMs;
        hashCode = 31 * hashCode + errorCode;
        hashCode = 31 * hashCode + (topics == null ? 0 : topics.hashCode());
        return hashCode;
    }
    
    @Override
    public FetchSnapshotResponseData duplicate() {
        FetchSnapshotResponseData _duplicate = new FetchSnapshotResponseData();
        _duplicate.throttleTimeMs = throttleTimeMs;
        _duplicate.errorCode = errorCode;
        ArrayList<TopicSnapshot> newTopics = new ArrayList<TopicSnapshot>(topics.size());
        for (TopicSnapshot _element : topics) {
            newTopics.add(_element.duplicate());
        }
        _duplicate.topics = newTopics;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "FetchSnapshotResponseData("
            + "throttleTimeMs=" + throttleTimeMs
            + ", errorCode=" + errorCode
            + ", topics=" + MessageUtil.deepToString(topics.iterator())
            + ")";
    }
    
    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }
    
    public short errorCode() {
        return this.errorCode;
    }
    
    public List<TopicSnapshot> topics() {
        return this.topics;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public FetchSnapshotResponseData setThrottleTimeMs(int v) {
        this.throttleTimeMs = v;
        return this;
    }
    
    public FetchSnapshotResponseData setErrorCode(short v) {
        this.errorCode = v;
        return this;
    }
    
    public FetchSnapshotResponseData setTopics(List<TopicSnapshot> v) {
        this.topics = v;
        return this;
    }
    
    public static class TopicSnapshot implements Message {
        String name;
        List<PartitionSnapshot> partitions;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("name", Type.COMPACT_STRING, "The name of the topic to fetch."),
                new Field("partitions", new CompactArrayOf(PartitionSnapshot.SCHEMA_0), "The partitions to fetch."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public TopicSnapshot(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public TopicSnapshot() {
            this.name = "";
            this.partitions = new ArrayList<PartitionSnapshot>(0);
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 0;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of TopicSnapshot");
            }
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field name was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field name had invalid length " + length);
                } else {
                    this.name = _readable.readString(length);
                }
            }
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field partitions was serialized as null");
                } else {
                    if (arrayLength > _readable.remaining()) {
                        throw new RuntimeException("Tried to allocate a collection of size " + arrayLength + ", but there are only " + _readable.remaining() + " bytes remaining.");
                    }
                    ArrayList<PartitionSnapshot> newCollection = new ArrayList<>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new PartitionSnapshot(_readable, _version));
                    }
                    this.partitions = newCollection;
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
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(name);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeUnsignedVarint(partitions.size() + 1);
            for (PartitionSnapshot partitionsElement : partitions) {
                partitionsElement.write(_writable, _cache, _version);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of TopicSnapshot");
            }
            {
                byte[] _stringBytes = name.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'name' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(name, _stringBytes);
                _size.addBytes(_stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));
            }
            {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(partitions.size() + 1));
                for (PartitionSnapshot partitionsElement : partitions) {
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
            _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TopicSnapshot)) return false;
            TopicSnapshot other = (TopicSnapshot) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
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
            hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
            hashCode = 31 * hashCode + (partitions == null ? 0 : partitions.hashCode());
            return hashCode;
        }
        
        @Override
        public TopicSnapshot duplicate() {
            TopicSnapshot _duplicate = new TopicSnapshot();
            _duplicate.name = name;
            ArrayList<PartitionSnapshot> newPartitions = new ArrayList<PartitionSnapshot>(partitions.size());
            for (PartitionSnapshot _element : partitions) {
                newPartitions.add(_element.duplicate());
            }
            _duplicate.partitions = newPartitions;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "TopicSnapshot("
                + "name=" + ((name == null) ? "null" : "'" + name.toString() + "'")
                + ", partitions=" + MessageUtil.deepToString(partitions.iterator())
                + ")";
        }
        
        public String name() {
            return this.name;
        }
        
        public List<PartitionSnapshot> partitions() {
            return this.partitions;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public TopicSnapshot setName(String v) {
            this.name = v;
            return this;
        }
        
        public TopicSnapshot setPartitions(List<PartitionSnapshot> v) {
            this.partitions = v;
            return this;
        }
    }
    
    public static class PartitionSnapshot implements Message {
        int index;
        short errorCode;
        SnapshotId snapshotId;
        LeaderIdAndEpoch currentLeader;
        long size;
        long position;
        BaseRecords unalignedRecords;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("index", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("snapshot_id", SnapshotId.SCHEMA_0, "The snapshot endOffset and epoch fetched"),
                new Field("size", Type.INT64, "The total size of the snapshot."),
                new Field("position", Type.INT64, "The starting byte position within the snapshot included in the Bytes field."),
                new Field("unaligned_records", Type.COMPACT_RECORDS, "Snapshot data in records format which may not be aligned on an offset boundary"),
                TaggedFieldsSection.of(
                    0, new Field("current_leader", LeaderIdAndEpoch.SCHEMA_0, "")
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public PartitionSnapshot(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public PartitionSnapshot() {
            this.index = 0;
            this.errorCode = (short) 0;
            this.snapshotId = new SnapshotId();
            this.currentLeader = new LeaderIdAndEpoch();
            this.size = 0L;
            this.position = 0L;
            this.unalignedRecords = null;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 0;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of PartitionSnapshot");
            }
            this.index = _readable.readInt();
            this.errorCode = _readable.readShort();
            {
                this.snapshotId = new SnapshotId(_readable, _version);
            }
            {
                this.currentLeader = new LeaderIdAndEpoch();
            }
            this.size = _readable.readLong();
            this.position = _readable.readLong();
            {
                int length;
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field unalignedRecords was serialized as null");
                } else {
                    this.unalignedRecords = _readable.readRecords(length);
                }
            }
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    case 0: {
                        this.currentLeader = new LeaderIdAndEpoch(_readable, _version);
                        break;
                    }
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _writable.writeInt(index);
            _writable.writeShort(errorCode);
            snapshotId.write(_writable, _cache, _version);
            if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                _numTaggedFields++;
            }
            _writable.writeLong(size);
            _writable.writeLong(position);
            _writable.writeUnsignedVarint(unalignedRecords.sizeInBytes() + 1);
            _writable.writeRecords(unalignedRecords);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            {
                if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                    _writable.writeUnsignedVarint(0);
                    _writable.writeUnsignedVarint(this.currentLeader.size(_cache, _version));
                    currentLeader.write(_writable, _cache, _version);
                }
            }
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of PartitionSnapshot");
            }
            _size.addBytes(4);
            _size.addBytes(2);
            {
                this.snapshotId.addSize(_size, _cache, _version);
            }
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
            _size.addBytes(8);
            _size.addBytes(8);
            {
                _size.addZeroCopyBytes(unalignedRecords.sizeInBytes());
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(unalignedRecords.sizeInBytes() + 1));
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
        public boolean equals(Object obj) {
            if (!(obj instanceof PartitionSnapshot)) return false;
            PartitionSnapshot other = (PartitionSnapshot) obj;
            if (index != other.index) return false;
            if (errorCode != other.errorCode) return false;
            if (this.snapshotId == null) {
                if (other.snapshotId != null) return false;
            } else {
                if (!this.snapshotId.equals(other.snapshotId)) return false;
            }
            if (this.currentLeader == null) {
                if (other.currentLeader != null) return false;
            } else {
                if (!this.currentLeader.equals(other.currentLeader)) return false;
            }
            if (size != other.size) return false;
            if (position != other.position) return false;
            if (!Objects.equals(this.unalignedRecords, other.unalignedRecords)) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + index;
            hashCode = 31 * hashCode + errorCode;
            hashCode = 31 * hashCode + (snapshotId == null ? 0 : snapshotId.hashCode());
            hashCode = 31 * hashCode + (currentLeader == null ? 0 : currentLeader.hashCode());
            hashCode = 31 * hashCode + ((int) (size >> 32) ^ (int) size);
            hashCode = 31 * hashCode + ((int) (position >> 32) ^ (int) position);
            hashCode = 31 * hashCode + Objects.hashCode(unalignedRecords);
            return hashCode;
        }
        
        @Override
        public PartitionSnapshot duplicate() {
            PartitionSnapshot _duplicate = new PartitionSnapshot();
            _duplicate.index = index;
            _duplicate.errorCode = errorCode;
            _duplicate.snapshotId = snapshotId.duplicate();
            _duplicate.currentLeader = currentLeader.duplicate();
            _duplicate.size = size;
            _duplicate.position = position;
            _duplicate.unalignedRecords = MemoryRecords.readableRecords(((MemoryRecords) unalignedRecords).buffer().duplicate());
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "PartitionSnapshot("
                + "index=" + index
                + ", errorCode=" + errorCode
                + ", snapshotId=" + snapshotId.toString()
                + ", currentLeader=" + currentLeader.toString()
                + ", size=" + size
                + ", position=" + position
                + ", unalignedRecords=" + unalignedRecords
                + ")";
        }
        
        public int index() {
            return this.index;
        }
        
        public short errorCode() {
            return this.errorCode;
        }
        
        public SnapshotId snapshotId() {
            return this.snapshotId;
        }
        
        public LeaderIdAndEpoch currentLeader() {
            return this.currentLeader;
        }
        
        public long size() {
            return this.size;
        }
        
        public long position() {
            return this.position;
        }
        
        public BaseRecords unalignedRecords() {
            return this.unalignedRecords;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public PartitionSnapshot setIndex(int v) {
            this.index = v;
            return this;
        }
        
        public PartitionSnapshot setErrorCode(short v) {
            this.errorCode = v;
            return this;
        }
        
        public PartitionSnapshot setSnapshotId(SnapshotId v) {
            this.snapshotId = v;
            return this;
        }
        
        public PartitionSnapshot setCurrentLeader(LeaderIdAndEpoch v) {
            this.currentLeader = v;
            return this;
        }
        
        public PartitionSnapshot setSize(long v) {
            this.size = v;
            return this;
        }
        
        public PartitionSnapshot setPosition(long v) {
            this.position = v;
            return this;
        }
        
        public PartitionSnapshot setUnalignedRecords(BaseRecords v) {
            this.unalignedRecords = v;
            return this;
        }
    }
    
    public static class SnapshotId implements Message {
        long endOffset;
        int epoch;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("end_offset", Type.INT64, ""),
                new Field("epoch", Type.INT32, ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public SnapshotId(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public SnapshotId() {
            this.endOffset = 0L;
            this.epoch = 0;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 0;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 0) {
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
            if (_version > 0) {
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
    
    public static class LeaderIdAndEpoch implements Message {
        int leaderId;
        int leaderEpoch;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("leader_id", Type.INT32, "The ID of the current leader or -1 if the leader is unknown."),
                new Field("leader_epoch", Type.INT32, "The latest known leader epoch"),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public LeaderIdAndEpoch(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public LeaderIdAndEpoch() {
            this.leaderId = 0;
            this.leaderEpoch = 0;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 0;
        }
        
        @Override
        public final void read(Readable _readable, short _version) {
            if (_version > 0) {
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
            if (_version > 0) {
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
}
