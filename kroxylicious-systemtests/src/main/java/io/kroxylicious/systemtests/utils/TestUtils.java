/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The type Test utils.
 */
public class TestUtils {

    private TestUtils() {
    }

    /**
     * Gets default posix file permissions.
     *
     * @return the default posix file permissions
     */
    public static FileAttribute<Set<PosixFilePermission>> getDefaultPosixFilePermissions() {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
    }

    /**
     * Gets resources URI.
     *
     * @param fileName the file name
     * @return the resources URI
     */
    @NonNull
    public static URI getResourcesURI(String fileName) {
        URI overrideFile;
        var resource = TestUtils.class.getClassLoader().getResource(fileName);
        try {
            if (resource == null) {
                throw new IllegalArgumentException("Cannot find resource " + fileName + " on classpath");
            }
            overrideFile = resource.toURI();
        }
        catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot determine file system path for " + resource, e);
        }
        return overrideFile;
    }

    /**
     * Is json valid boolean.
     *
     * @param jsonInString the json in string
     * @return the boolean
     */
    public static boolean isJSONValid(String jsonInString) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(jsonInString);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Gets value from json.
     *
     * @param json the json
     * @param propertyName the property name
     * @return the value from json
     */
    public static String getValueFromJson(String json, String propertyName) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node;
        try {
            node = mapper.readTree(json);
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        return node.get(propertyName).toString();
    }
}
