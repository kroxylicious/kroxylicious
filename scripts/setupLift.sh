#!/usr/bin/env bash

#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

mvn install -DskipTests -pl :kroxylicious-krpc-plugin
tree "${M2_HOME}/repository/io/kroxylicious"