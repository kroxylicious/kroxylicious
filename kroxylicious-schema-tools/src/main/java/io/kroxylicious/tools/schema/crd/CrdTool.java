/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.crd;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.kroxylicious.tools.schema.model.SchemaObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CrdTool {

    private final YAMLMapper mapper;
    private final List<Path> srcPaths;

    public CrdTool(List<Path> srcPaths) {
        this.mapper = new YAMLMapper();
        this.srcPaths = srcPaths;
    }

    public void run() {
        srcPaths.stream()
                .map(src -> {
                    try {
                        return mapper.readValue(src.toFile(), SchemaObject.class);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .map(schema -> {
                    // name (singular, plural, kind, version)
                    // subresources
                    // schema for each version
                }).forEach(x -> {});
    }

    public static void main(String[] args) {

    }
}
