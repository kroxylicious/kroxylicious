/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.executor;

import java.io.Serializable;

public class ExecResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int returnCode;
    private final String stdOut;
    private final String stdErr;

    ExecResult(int returnCode, String stdOut, String stdErr) {
        this.returnCode = returnCode;
        this.stdOut = stdOut;
        this.stdErr = stdErr;
    }

    public boolean exitStatus() {
        return returnCode == 0;
    }

    public int returnCode() {
        return returnCode;
    }

    public String out() {
        return stdOut;
    }

    public String err() {
        return stdErr;
    }
}
