/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

/**
 * Class with various utility methods for reading and writing files and objects
 */
public final class ReadWriteUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final YAMLFactory WRITE_YAML_FACTORY = YAMLFactory.builder()
            .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
            .build();
    private static final ObjectMapper READ_YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper WRITE_YAML_OBJECT_MAPPER = new ObjectMapper(WRITE_YAML_FACTORY);

    private ReadWriteUtils() {
        // All static methods
    }

    /**
     * Read the classpath resource with the given resourceName and return the URI
     *
     * @param cls           The class relative to which the resource will be loaded.
     * @param resourceName  The name of the file stored in resources
     *
     * @return  The URI of the resource
     */
    public static URI getResourceURI(Class<?> cls, String resourceName) {
        URL url = null;
        try {
            url = cls.getClassLoader().getResource(resourceName);
            if (url == null) {
                throw new IllegalArgumentException("Cannot find resource " + resourceName + " on classpath");
            }
            return url.toURI();
        }
        catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot determine file system path for " + url, e);
        }
    }

    /**
     * Reads an object from a YAML file
     *
     * @param yamlFile   File with the YAML
     * @param c          The class representing the object
     *
     * @return  Returns the object instance based on the YAML file
     *
     * @param <T>   Type of the object
     */
    public static <T> T readObjectFromYamlFilepath(File yamlFile, Class<T> c) {
        try {
            return READ_YAML_OBJECT_MAPPER.readValue(yamlFile, c);
        }
        catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads an object from a path to a YAML file
     *
     * @param yamlPath   Path to a YAML file
     * @param c          The class representing the object
     *
     * @return  Returns the object instance based on the YAML file path
     *
     * @param <T>   Type of the object
     */
    public static <T> T readObjectFromYamlFilepath(String yamlPath, Class<T> c) {
        return readObjectFromYamlFilepath(new File(yamlPath), c);
    }

    /**
     * Converts an object into YAML
     *
     * @param instance  The resource that should be converted to YAML
     *
     * @return  String with the YAML representation of the object
     *
     * @param <T>   Type of the object
     */
    public static <T> String writeObjectToYamlString(T instance, int indent) {
        try {
            return WRITE_YAML_OBJECT_MAPPER.writeValueAsString(instance).indent(indent).trim();
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Is valid json.
     *
     * @param value the value
     * @return the boolean
     */
    public static boolean isValidJson(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        try {
            OBJECT_MAPPER.readTree(value);
        }
        catch (IOException e) {
            return false;
        }
        return true;
    }

    /**
     * Writes text into a file
     *
     * @param filePath  Path of the file where the text will be written
     * @param text      The text that will be written into the file
     */
    public static void writeFile(Path filePath, String text) {
        try {
            Files.writeString(filePath, text, Charset.defaultCharset());
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to generate file on Path: " + filePath, e);
        }
    }

    /**
     * Creates an empty file in the default temporary-file directory, using the given prefix and suffix.
     *
     * @param prefix    The prefix of the empty file (default: UUID).
     * @param suffix    The suffix of the empty file (default: .tmp).
     *
     * @return The empty file just created.
     */
    public static File tempFile(String prefix, String suffix) {
        File file;
        prefix = prefix == null ? UUID.randomUUID().toString() : prefix;
        suffix = suffix == null ? ".tmp" : suffix;
        try {
            file = Files.createTempFile(prefix, suffix, getDefaultPosixFilePermissions()).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        file.deleteOnExit();
        return file;
    }

    private static FileAttribute<Set<PosixFilePermission>> getDefaultPosixFilePermissions() {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
    }
}
