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
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.umd.cs.findbugs.annotations.NonNull;
import info.schnatterer.mobynamesgenerator.MobyNamesGenerator;

/**
 * The type Test utils.
 */
public class TestUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern IMAGE_PATTERN_FULL_PATH = Pattern.compile("^(?<registry>[^/]*)/(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");
    private static final Pattern IMAGE_PATTERN = Pattern.compile("^(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");

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
     * Gets random suffix to be added to a pod Name.
     *
     * @return the random pod name
     */
    public static String getRandomPodNameSuffix() {
        return MobyNamesGenerator.getRandomName().replace("_", "-");
    }

    /**
     * Gets json file content.
     *
     * @param fileName the file name
     * @return the json file content
     */
    public static String getJsonFileContent(String fileName) {
        try {
            return OBJECT_MAPPER.readTree(new File(Path.of(getResourcesURI(fileName)).toString())).toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // TO BE REMOVED when introduced in ImageUtils from TestFrame project

    /**
     * Change registry org image and tag.
     *
     * @param imageRepo the image repo
     * @param newRegistry the new registry
     * @param newOrg the new org
     * @param newImage the new image
     * @param newTag the new tag
     * @return the string
     */
    public static String changeRegistryOrgImageAndTag(String imageRepo, String newRegistry, String newOrg, String newImage, String newTag) {
        Matcher m = IMAGE_PATTERN_FULL_PATH.matcher(imageRepo);
        if (m.find()) {
            String registry = setImagePropertiesIfNeeded(m.group("registry"), newRegistry);
            String org = setImagePropertiesIfNeeded(m.group("org"), newOrg);
            String tag = setImagePropertiesIfNeeded(m.group("tag"), newTag);
            String image = setImagePropertiesIfNeeded(m.group("image"), newImage);
            ;
            String newImageRepo = registry + "/" + org + "/" + image + ":" + tag;
            LOGGER.info("Updating container image to {}", newImageRepo);
            return newImageRepo;
        }
        else {
            m = IMAGE_PATTERN.matcher(imageRepo);
            if (m.find()) {
                String registry = newRegistry != null ? newRegistry + "/" : "";
                String org = setImagePropertiesIfNeeded(m.group("org"), newOrg);
                String tag = setImagePropertiesIfNeeded(m.group("tag"), newTag);
                String image = setImagePropertiesIfNeeded(m.group("image"), newImage);
                ;
                String newImageRepo = registry + org + "/" + image + ":" + tag;
                LOGGER.info("Updating container image to {}", newImageRepo);
                return newImageRepo;
            }
            else {
                return imageRepo;
            }
        }
    }

    private static String setImagePropertiesIfNeeded(String currentValue, String newValue) {
        return newValue != null && !newValue.isEmpty() && !currentValue.equals(newValue) ? newValue : currentValue;
    }
}
