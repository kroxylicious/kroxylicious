/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.List;

public class InvalidRulesFileException extends RuntimeException {
    private final List<String> errorMessages;

    public InvalidRulesFileException(String message, List<String> errorMessages) {
        super(message);
        this.errorMessages = errorMessages;
    }

    String errors() {
        StringBuilder sb = new StringBuilder();
        for (var error : this.errorMessages) {
            sb.append(error).append(System.lineSeparator());
        }
        sb.setLength(sb.length() - System.lineSeparator().length());
        return sb.toString();
    }

}
