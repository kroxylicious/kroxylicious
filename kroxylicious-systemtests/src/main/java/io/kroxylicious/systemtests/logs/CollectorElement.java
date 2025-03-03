/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.logs;

import java.util.Objects;

public class CollectorElement {

    private final String testClassName;
    private String testMethodName;

    public CollectorElement(String testClass, String testTest) {
        this.testClassName = testClass;
        this.testMethodName = testTest;
    }

    public CollectorElement(String testClass) {
        this.testClassName = testClass;
        this.testMethodName = "";
    }

    public static CollectorElement createCollectorElement(String testClass, String testMethod) {
        return new CollectorElement(testClass, testMethod);
    }

    public boolean isEmpty() {
        return this.testClassName.isEmpty() && this.testMethodName.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CollectorElement that = (CollectorElement) o;
        return Objects.equals(testClassName, that.testClassName) && Objects.equals(testMethodName, that.testMethodName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(testClassName, testMethodName);
    }
}
