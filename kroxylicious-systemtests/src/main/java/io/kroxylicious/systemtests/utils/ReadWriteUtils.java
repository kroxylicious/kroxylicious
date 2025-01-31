/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Class with various utility methods for reading and writing files and objects
 */
public final class ReadWriteUtils {
    private static final ObjectMapper READ_YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    private ReadWriteUtils() {
        // All static methods
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
}
