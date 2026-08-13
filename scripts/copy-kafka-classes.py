#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""
Copy non-generated Kafka classes from a local Kafka checkout into kroxylicious-api.

Usage:
    python3 scripts/copy-kafka-classes.py <kafka-repo-root>

Arguments:
    kafka-repo-root   Path to a local clone of the Apache Kafka repository,
                      checked out at tag 4.3.0
                      (commit a9ce3221537b8653448750697915607dc7936cf3).

The target directory is always:
    kroxylicious-api/src/main/java/io/kroxylicious/kafka/

relative to the root of this (Kroxylicious) repository.

Package mapping: org.apache.kafka.* -> io.kroxylicious.kafka.* (prefix swap only).
The single exception is org.apache.kafka.common.utils.internals.*, which is flattened
to io.kroxylicious.kafka.common.utils.* (the internals subpackage is not reproduced).

The file list and import-rewrite rules encode the specific decisions made for
this initial copy (see issue #4578 / proposal 116).  They are intentionally
not configurable; reviewers should read and understand them here.
"""

import os
import sys

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

def _repo_root():
    """Return the Kroxylicious repo root (parent of the scripts/ directory)."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _kafka_clients(kafka_root):
    return os.path.join(kafka_root,
                        "clients", "src", "main", "java", "org", "apache", "kafka")


def _target_base(repo_root):
    return os.path.join(repo_root, "kroxylicious-api", "src", "main", "java",
                        "io", "kroxylicious", "kafka")


# ---------------------------------------------------------------------------
# File list
# (kafka_path_relative_to_clients/src/main/java/org/apache/kafka,
#  target_path_relative_to_io/kroxylicious/kafka)
#
# For most files the source and target relative paths are identical — the only
# thing that changes is the package prefix (org.apache.kafka -> io.kroxylicious.kafka).
# Exceptions: utils.internals/* sources land under common/utils/ (no internals subpackage).
# ---------------------------------------------------------------------------

FILES = [
    # --- common/protocol ---
    ("common/protocol/ApiMessage.java",              "common/protocol/ApiMessage.java"),
    ("common/protocol/ByteBufferAccessor.java",      "common/protocol/ByteBufferAccessor.java"),
    ("common/protocol/Message.java",                 "common/protocol/Message.java"),
    ("common/protocol/MessageSizeAccumulator.java",  "common/protocol/MessageSizeAccumulator.java"),
    ("common/protocol/MessageUtil.java",             "common/protocol/MessageUtil.java"),
    ("common/protocol/ObjectSerializationCache.java","common/protocol/ObjectSerializationCache.java"),
    ("common/protocol/Readable.java",                "common/protocol/Readable.java"),
    ("common/protocol/Writable.java",                "common/protocol/Writable.java"),

    # --- common/protocol/types ---
    ("common/protocol/types/ArrayOf.java",               "common/protocol/types/ArrayOf.java"),
    ("common/protocol/types/BoundField.java",            "common/protocol/types/BoundField.java"),
    ("common/protocol/types/CompactArrayOf.java",        "common/protocol/types/CompactArrayOf.java"),
    ("common/protocol/types/Field.java",                 "common/protocol/types/Field.java"),
    ("common/protocol/types/NullableSchema.java",        "common/protocol/types/NullableSchema.java"),
    ("common/protocol/types/RawTaggedField.java",        "common/protocol/types/RawTaggedField.java"),
    ("common/protocol/types/RawTaggedFieldWriter.java",  "common/protocol/types/RawTaggedFieldWriter.java"),
    ("common/protocol/types/Schema.java",                "common/protocol/types/Schema.java"),
    ("common/protocol/types/SchemaException.java",       "common/protocol/types/SchemaException.java"),
    ("common/protocol/types/Struct.java",                "common/protocol/types/Struct.java"),
    ("common/protocol/types/TaggedFields.java",          "common/protocol/types/TaggedFields.java"),
    ("common/protocol/types/Type.java",                  "common/protocol/types/Type.java"),

    # --- common/record/internal ---
    # Skipped (server-side / FileChannel deps): FileRecords, FileLogInputStream,
    # UnalignedFileRecords, RemoteLogInputStream, package-info.
    # DefaultRecordBatch and AbstractLegacyRecordBatch require surgical removal of
    # inner classes that reference FileLogInputStream (see issue #4578).
    # ControlRecordType: ControlRecordTypeSchema import kept as org.apache.kafka.* (TODO, see issue #4644).
    # ControlRecordUtils / EndTransactionMarker / MemoryRecordsBuilder: deferred to PR3.
    # They depend on io.kroxylicious.kafka.common.message.* types produced by the message generator,
    # which is not available until PR3 (issue #4644).
    ("common/record/internal/AbstractLegacyRecordBatch.java", "common/record/internal/AbstractLegacyRecordBatch.java"),
    ("common/record/internal/ControlRecordType.java",         "common/record/internal/ControlRecordType.java"),
    ("common/record/internal/AbstractRecordBatch.java",        "common/record/internal/AbstractRecordBatch.java"),
    ("common/record/internal/AbstractRecords.java",            "common/record/internal/AbstractRecords.java"),
    ("common/record/internal/BaseRecords.java",                "common/record/internal/BaseRecords.java"),
    ("common/record/internal/ByteBufferLogInputStream.java",   "common/record/internal/ByteBufferLogInputStream.java"),
    ("common/record/internal/CompressionRatioEstimator.java",  "common/record/internal/CompressionRatioEstimator.java"),
    ("common/record/internal/CompressionType.java",            "common/record/internal/CompressionType.java"),
    ("common/record/internal/DefaultRecord.java",              "common/record/internal/DefaultRecord.java"),
    ("common/record/internal/DefaultRecordBatch.java",         "common/record/internal/DefaultRecordBatch.java"),
    ("common/record/internal/DefaultRecordsSend.java",         "common/record/internal/DefaultRecordsSend.java"),
    ("common/record/internal/LegacyRecord.java",               "common/record/internal/LegacyRecord.java"),
    ("common/record/internal/LogInputStream.java",             "common/record/internal/LogInputStream.java"),
    ("common/record/internal/MemoryRecords.java",              "common/record/internal/MemoryRecords.java"),
    ("common/record/internal/MultiRecordsSend.java",           "common/record/internal/MultiRecordsSend.java"),
    ("common/record/internal/MutableRecordBatch.java",         "common/record/internal/MutableRecordBatch.java"),
    ("common/record/internal/PartialDefaultRecord.java",       "common/record/internal/PartialDefaultRecord.java"),
    ("common/record/internal/Record.java",                     "common/record/internal/Record.java"),
    ("common/record/internal/RecordBatch.java",                "common/record/internal/RecordBatch.java"),
    ("common/record/internal/RecordBatchIterator.java",        "common/record/internal/RecordBatchIterator.java"),
    ("common/record/internal/RecordVersion.java",              "common/record/internal/RecordVersion.java"),
    ("common/record/internal/Records.java",                    "common/record/internal/Records.java"),
    ("common/record/internal/RecordsSend.java",                "common/record/internal/RecordsSend.java"),
    ("common/record/internal/SimpleRecord.java",               "common/record/internal/SimpleRecord.java"),
    ("common/record/internal/TransferableRecords.java",        "common/record/internal/TransferableRecords.java"),
    ("common/record/internal/UnalignedMemoryRecords.java",     "common/record/internal/UnalignedMemoryRecords.java"),
    ("common/record/internal/UnalignedRecords.java",           "common/record/internal/UnalignedRecords.java"),

    # --- common/record ---
    ("common/record/TimestampType.java", "common/record/TimestampType.java"),

    # --- common ---
    ("common/Uuid.java",                   "common/Uuid.java"),
    ("common/KafkaException.java",         "common/KafkaException.java"),
    ("common/InvalidRecordException.java", "common/InvalidRecordException.java"),

    # --- common/errors ---
    # ApiException is part of the public Kroxylicious API
    # (used in KafkaProxyExceptionMapper.errorResponseForMessage).
    ("common/errors/ApiException.java",                  "common/errors/ApiException.java"),
    ("common/errors/CorruptRecordException.java",        "common/errors/CorruptRecordException.java"),
    ("common/errors/InvalidConfigurationException.java", "common/errors/InvalidConfigurationException.java"),

    # --- common/annotation ---
    ("common/annotation/InterfaceAudience.java", "common/annotation/InterfaceAudience.java"),

    # --- common/header ---
    ("common/header/Header.java",                 "common/header/Header.java"),
    ("common/header/internals/RecordHeader.java", "common/header/internals/RecordHeader.java"),

    # --- common/network ---
    ("common/network/Send.java",               "common/network/Send.java"),
    ("common/network/TransferableChannel.java", "common/network/TransferableChannel.java"),

    # --- common/utils ---
    # Utils is copied partially: file/OS-handling methods (flushDir, flushFile, atomicMoveWithFallback,
    # readFile, delete, readFully(FileChannel), writeFully) and config-framework methods
    # (propsToMap, castToStringObjectMap, ensureConcreteSubclass, mergeConfigs) are omitted
    # because they pull in ConfigDef/ConfigException/OperatingSystem which are out of scope.
    ("common/utils/Utils.java", "common/utils/Utils.java"),

    # --- common/utils (from utils.internals in Kafka 4.3.0) ---
    # Kafka 4.3.0 moved these from common.utils to common.utils.internals (KAFKA-20128).
    # We flatten them directly into io.kroxylicious.kafka.common.utils (no internals subpackage).
    ("common/utils/internals/AbstractIterator.java",      "common/utils/AbstractIterator.java"),
    ("common/utils/internals/BufferSupplier.java",         "common/utils/BufferSupplier.java"),
    ("common/utils/internals/ByteBufferInputStream.java",  "common/utils/ByteBufferInputStream.java"),
    ("common/utils/internals/ByteBufferOutputStream.java", "common/utils/ByteBufferOutputStream.java"),
    ("common/utils/internals/ByteUtils.java",              "common/utils/ByteUtils.java"),
    ("common/utils/internals/Checksums.java",              "common/utils/Checksums.java"),
    ("common/utils/internals/ChunkedBytesStream.java",     "common/utils/ChunkedBytesStream.java"),
    ("common/utils/internals/CloseableIterator.java",      "common/utils/CloseableIterator.java"),
    ("common/utils/internals/Crc32C.java",                 "common/utils/Crc32C.java"),

    # --- common/compress ---
    ("common/compress/Compression.java",         "common/compress/Compression.java"),
    ("common/compress/GzipCompression.java",     "common/compress/GzipCompression.java"),
    ("common/compress/GzipOutputStream.java",    "common/compress/GzipOutputStream.java"),
    ("common/compress/Lz4BlockInputStream.java", "common/compress/Lz4BlockInputStream.java"),
    ("common/compress/Lz4BlockOutputStream.java","common/compress/Lz4BlockOutputStream.java"),
    ("common/compress/Lz4Compression.java",      "common/compress/Lz4Compression.java"),
    ("common/compress/NoCompression.java",       "common/compress/NoCompression.java"),
    ("common/compress/SnappyCompression.java",   "common/compress/SnappyCompression.java"),
    ("common/compress/ZstdCompression.java",     "common/compress/ZstdCompression.java"),
]


# ---------------------------------------------------------------------------
# Rewrite rules
# ---------------------------------------------------------------------------

# Package declaration rewrites.
# Rule: org.apache.kafka.* -> io.kroxylicious.kafka.* (prefix swap).
# Exception: utils.internals is flattened to utils (no internals subpackage).
# Ordering: more-specific prefixes first (internals before utils, header.internals
# before header, record.internal before record, protocol.types before protocol).
PACKAGE_REWRITES = [
    ("package org.apache.kafka.common.utils.internals;",  "package io.kroxylicious.kafka.common.utils;"),
    ("package org.apache.kafka.common.compress;",         "package io.kroxylicious.kafka.common.compress;"),
    ("package org.apache.kafka.common.protocol.types;",   "package io.kroxylicious.kafka.common.protocol.types;"),
    ("package org.apache.kafka.common.protocol;",         "package io.kroxylicious.kafka.common.protocol;"),
    ("package org.apache.kafka.common.record.internal;",  "package io.kroxylicious.kafka.common.record.internal;"),
    ("package org.apache.kafka.common.record;",           "package io.kroxylicious.kafka.common.record;"),
    ("package org.apache.kafka.common.errors;",           "package io.kroxylicious.kafka.common.errors;"),
    ("package org.apache.kafka.common.annotation;",       "package io.kroxylicious.kafka.common.annotation;"),
    ("package org.apache.kafka.common.header.internals;", "package io.kroxylicious.kafka.common.header.internals;"),
    ("package org.apache.kafka.common.header;",           "package io.kroxylicious.kafka.common.header;"),
    ("package org.apache.kafka.common.network;",          "package io.kroxylicious.kafka.common.network;"),
    ("package org.apache.kafka.common;",                  "package io.kroxylicious.kafka.common;"),
]

# Import prefix rewrites.
# Ordering: more-specific prefixes first, same reason as above.
# Imports of classes that remain in org.apache.kafka.* (generated message.* types
# in the 4 deferred files) are left untouched; those files are not in the copy list.
IMPORT_PREFIXES = [
    # utils.internals -> flatten to common.utils (no internals subpackage)
    ("org.apache.kafka.common.utils.internals.",           "io.kroxylicious.kafka.common.utils."),
    # compress
    ("org.apache.kafka.common.compress.",                  "io.kroxylicious.kafka.common.compress."),
    # Utils — copied partially (config/file-IO methods omitted, see FILES comment)
    ("org.apache.kafka.common.utils.Utils",                  "io.kroxylicious.kafka.common.utils.Utils"),
    # protocol.types before protocol
    ("org.apache.kafka.common.protocol.types.",            "io.kroxylicious.kafka.common.protocol.types."),
    ("org.apache.kafka.common.protocol.",                  "io.kroxylicious.kafka.common.protocol."),
    # record.internal before record (covers ControlRecordType, MemoryRecordsBuilder, etc.)
    ("org.apache.kafka.common.record.internal.",           "io.kroxylicious.kafka.common.record.internal."),
    ("org.apache.kafka.common.record.TimestampType",       "io.kroxylicious.kafka.common.record.TimestampType"),
    # Handles any pre-4.3.0 style import of CompressionType at its old (non-internal) path
    ("org.apache.kafka.common.record.CompressionType",     "io.kroxylicious.kafka.common.record.internal.CompressionType"),
    # errors — individual classes (specific before any hypothetical broad errors.* rule)
    ("org.apache.kafka.common.errors.CorruptRecordException",
                                                           "io.kroxylicious.kafka.common.errors.CorruptRecordException"),
    ("org.apache.kafka.common.errors.InvalidConfigurationException",
                                                           "io.kroxylicious.kafka.common.errors.InvalidConfigurationException"),
    ("org.apache.kafka.common.errors.ApiException",        "io.kroxylicious.kafka.common.errors.ApiException"),
    # annotation
    ("org.apache.kafka.common.annotation.",                "io.kroxylicious.kafka.common.annotation."),
    # header.internals before header
    ("org.apache.kafka.common.header.internals.",          "io.kroxylicious.kafka.common.header.internals."),
    ("org.apache.kafka.common.header.",                    "io.kroxylicious.kafka.common.header."),
    # network
    ("org.apache.kafka.common.network.",                   "io.kroxylicious.kafka.common.network."),
    # common top-level classes
    ("org.apache.kafka.common.KafkaException",             "io.kroxylicious.kafka.common.KafkaException"),
    ("org.apache.kafka.common.InvalidRecordException",     "io.kroxylicious.kafka.common.InvalidRecordException"),
    ("org.apache.kafka.common.Uuid",                       "io.kroxylicious.kafka.common.Uuid"),
]


# ---------------------------------------------------------------------------
# Transformation logic
# ---------------------------------------------------------------------------

def _rewrite_line(line):
    stripped = line.strip()

    if stripped.startswith("package "):
        for old, new in PACKAGE_REWRITES:
            if stripped == old:
                return line.replace(old, new)
        return line

    if stripped.startswith("import "):
        rest = stripped[len("import "):]
        is_static = rest.startswith("static ")
        if is_static:
            rest = rest[len("static "):]
        rest = rest.rstrip(";").strip()

        for old_prefix, new_prefix in IMPORT_PREFIXES:
            if rest == old_prefix or rest.startswith(old_prefix):
                rewritten = new_prefix + rest[len(old_prefix):]
                return line.replace(rest, rewritten, 1)

    return line


def _transform(content):
    return "".join(_rewrite_line(line) for line in content.splitlines(keepends=True))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0 if sys.argv[1:] and sys.argv[1] in ("-h", "--help") else 1)

    kafka_root = os.path.abspath(sys.argv[1])
    kafka_clients = _kafka_clients(kafka_root)
    repo_root = _repo_root()
    target_base = _target_base(repo_root)

    if not os.path.isdir(kafka_clients):
        print(f"error: {kafka_clients} does not exist", file=sys.stderr)
        print(f"       Is '{kafka_root}' the root of a Kafka checkout?", file=sys.stderr)
        sys.exit(1)

    errors = []
    for kafka_rel, target_rel in FILES:
        src = os.path.join(kafka_clients, kafka_rel)
        dst = os.path.join(target_base, target_rel)

        if not os.path.exists(src):
            errors.append(f"MISSING SOURCE: {src}")
            continue

        os.makedirs(os.path.dirname(dst), exist_ok=True)

        with open(src, "r", encoding="utf-8") as f:
            original = f.read()

        transformed = _transform(original)

        with open(dst, "w", encoding="utf-8") as f:
            f.write(transformed)

        # Sanity check: flag any old package declaration that survived
        for old_pkg, _ in PACKAGE_REWRITES:
            if old_pkg in transformed:
                errors.append(f"UNREWRITTEN PACKAGE in {target_rel}: still contains '{old_pkg}'")

        print(f"  OK  {target_rel}")

    print(f"\nCopied {len(FILES) - len(errors)} / {len(FILES)} files.")
    if errors:
        print("\nERRORS:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
