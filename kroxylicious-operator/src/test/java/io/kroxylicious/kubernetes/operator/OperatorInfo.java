/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

record OperatorInfo(String imageName, String imageArchive) {

    static OperatorInfo fromResource() {
        try (var is = OperatorInfo.class.getResourceAsStream("/operator-info.properties")) {
            var properties = new Properties();
            properties.load(is);
            String imageName = properties.getProperty("operator.image.name");
            String imageArchive = properties.getProperty("operator.image.archive");
            return new OperatorInfo(imageName, imageArchive);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
