/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.vendoring.rewrite;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.openrewrite.Recipe;

/**
 * Pointless comment to keep the linter happy.
 * Base class to avoid stupid sonar duplication warnings.
 */
public class BaseVendoringRecipe extends Recipe {

    private final String displayName;
    private final String description;

    /**
     * Constructs the instance by providing the standard fields.
     * @param displayName the name that appears in the logs.
     * @param description the description of this recipe
     */
    public BaseVendoringRecipe(String displayName, String description) {
        super();
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    @NonNull
    public String getDisplayName() {
        return displayName;
    }

    @Override
    @NonNull
    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        // recipe equality is purely based on classname. Let's stick to that
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        // recipe equality is purely based on classname. Let's stick to that
        return super.hashCode();
    }
}
