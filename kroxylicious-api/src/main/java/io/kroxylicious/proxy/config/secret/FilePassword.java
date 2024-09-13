/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.secret;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A reference to the file containing a plain text password in UTF-8 encoding.  If the password file
 * contains more than one line, only the characters of the first line are taken to be the password,
 * excluding the line ending.  Subsequent lines are ignored.
 * <br>
 * Note that the {@code filePath} alias was deprecated at 0.5.0.
 *
 * @param passwordFile file containing the password.
 */
public record FilePassword(
        @JsonProperty(required = true) @JsonAlias("filePath")
        String passwordFile
) implements PasswordProvider {

    public FilePassword {
        Objects.requireNonNull(passwordFile);
    }

    @Override
    public String getProvidedPassword() {
        return readPasswordFile(passwordFile);
    }

    @Override
    public String toString() {
        return "FilePassword["
               +
               "passwordFile="
               + passwordFile
               + ']';
    }

    @NonNull
    static String readPasswordFile(String passwordFile) {
        try (var fr = new BufferedReader(new FileReader(passwordFile, StandardCharsets.UTF_8))) {
            return fr.readLine();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Exception reading " + passwordFile, e);
        }
    }

}
