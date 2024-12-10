/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.model;

import java.net.URI;

import edu.umd.cs.findbugs.annotations.NonNull;

public class SchemaVisitor {

    public static class Context {
        private final Object parent;
        private final String keyword;
        private final String path;

        Context(
                Context parent,
                String keyword,
                String path) {
            this.parent = parent;
            this.keyword = keyword;
            this.path = path;
        }

        Context(
                URI base) {
            this.parent = base;
            this.keyword = "";
            this.path = "";
        }

        public URI base() {
            if (parent instanceof URI uri) {
                return uri;
            }
            else {
                return ((Context) parent).base();
            }
        }

        public String keyword() {
            return keyword;
        }

        public String fullPath() {
            if (parent instanceof Context parentContext) {
                return parentContext.fullPath() + "/" + path;
            }
            else {
                return path;
            }
        }

        public boolean isRootSchema() {
            return parent instanceof URI;
        }

        @Override
        public String toString() {
            return "Context{" +
                    (parent instanceof URI ? "base=" + parent + ", " : "") +
                    "keyword='" + keyword + '\'' +
                    ", fullPath='" + fullPath() + '\'' +
                    '}';
        }
    }

    public void enterSchema(Context context, @NonNull SchemaObject schema) {
        // default behaviour is no-op
    }

    public void exitSchema(Context context, @NonNull SchemaObject schema) {
        // default behaviour is no-op
    }
}
