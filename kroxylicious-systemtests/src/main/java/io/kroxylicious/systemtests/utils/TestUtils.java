/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 * The type Test utils.
 */
public class TestUtils {

    /**
     * Gets default posix file permissions.
     *
     * @return the default posix file permissions
     */
    public static FileAttribute<Set<PosixFilePermission>> getDefaultPosixFilePermissions() {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
    }

    /**
     * Concat string if value don't exist.
     *
     * @param originalString the original String
     * @param stringToConcat the string to concat
     * @param stringSeparator the string separator
     * @return the string
     */
    public static String concatStringIfValueDontExist(String originalString, String stringToConcat, String stringSeparator) {
        if (originalString.isEmpty() || originalString.isBlank()) {
            stringSeparator = "";
        }

        if (!originalString.contains(stringToConcat)) {
            originalString = originalString.concat(stringSeparator + stringToConcat);
        }
        return originalString;
    }
}
