/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.executor;

import java.io.Serializable;

/**
 * The type Exec result.
 */
public class ExecResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int returnCode;
    private final String stdOut;
    private final String stdErr;

    /**
     * Instantiates a new Exec result.
     *
     * @param returnCode the return code
     * @param stdOut the std out
     * @param stdErr the std err
     */
    public ExecResult(int returnCode, String stdOut, String stdErr) {
        this.returnCode = returnCode;
        this.stdOut = stdOut;
        this.stdErr = stdErr;
    }

    /**
     * Exit status boolean.
     *
     * @return the boolean
     */
    public boolean exitStatus() {
        return returnCode == 0;
    }

    /**
     * Return code int.
     *
     * @return the int
     */
    public int returnCode() {
        return returnCode;
    }

    /**
     * Out string.
     *
     * @return the string
     */
    public String out() {
        return stdOut;
    }

    /**
     * Err string.
     *
     * @return the string
     */
    public String err() {
        return stdErr;
    }
}
