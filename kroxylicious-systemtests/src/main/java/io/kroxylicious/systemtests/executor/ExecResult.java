/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.executor;

import java.io.Serializable;

/**
 * The type Exec result.
 * It is serializable because it is used in KubeClusterException that is serializable
 */
public class ExecResult implements Serializable {
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
     * Return true if the result is success, false otherwise.
     *
     * @return the boolean
     */
    public boolean isSuccess() {
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
     * Returns the stdOut string.
     *
     * @return the string
     */
    public String out() {
        return stdOut;
    }

    /**
     * Returns the stdErr string.
     *
     * @return the string
     */
    public String err() {
        return stdErr;
    }
}
