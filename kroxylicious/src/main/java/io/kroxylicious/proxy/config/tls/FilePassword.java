/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonCreator;

public record FilePassword(String passwordFile) implements PasswordProvider {
    @JsonCreator
    public FilePassword {
    }

    @Override
    public String getProvidedPassword() {
        if (passwordFile == null) {
            return null;
        }
        try (var fr = new BufferedReader(new FileReader(passwordFile, StandardCharsets.UTF_8))) {
            return fr.readLine();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString() {
        return "FilePassword[" +
                "passwordFile=" + passwordFile + ']';
    }

}
