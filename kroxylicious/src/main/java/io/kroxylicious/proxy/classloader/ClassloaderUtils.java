/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.classloader;

import java.util.Optional;

public class ClassloaderUtils {
    public static <T> Optional<Class<? extends T>> loadClassIfPresent(Class<T> superClass, String targetClassName) {
        try {
            Class<?> targetClass = ClassloaderUtils.class.getClassLoader().loadClass(targetClassName);
            if (superClass.isAssignableFrom(targetClass)) {
                return Optional.of(targetClass.asSubclass(superClass));
            }
            else {
                throw new RuntimeException("class " + targetClass.getSimpleName() + " is not assignable to " + superClass);
            }
        }
        catch (ClassNotFoundException e) {
            return Optional.empty();
        }
    }

}
