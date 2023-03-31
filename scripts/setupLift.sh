#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

M2_HOME=~/.m2

mkdir -p $M2_HOME
cat <<EOF > $M2_HOME/settings.xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                      https://maven.apache.org/xsd/settings-1.0.0.xsd">
  <localRepository>$M2_HOME/repository</localRepository>
  <interactiveMode>false</interactiveMode>
  <offline>false</offline>
</settings>
EOF

mvn install -DskipTests -pl :kroxylicious-krpc-plugin -Dquick
