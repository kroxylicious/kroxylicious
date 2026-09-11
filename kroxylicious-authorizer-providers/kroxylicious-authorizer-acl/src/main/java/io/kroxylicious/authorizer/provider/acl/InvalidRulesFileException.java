/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.List;

import io.kroxylicious.authorizer.service.AuthorizerException;

/**
 * Thrown when an ACL rules file cannot be parsed, or is otherwise invalid.
 */
public class InvalidRulesFileException extends AuthorizerException {
    /** The individual error messages describing why the rules file was invalid. */
    private final List<String> errorMessages;

    /**
     * Constructs an InvalidRulesFileException.
     * @param message the exception message.
     * @param errorMessages the individual error messages describing why the rules file was invalid.
     */
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
