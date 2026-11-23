#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import xml.etree.ElementTree as ET

settings_file = sys.argv[1]
tree = ET.parse(settings_file)
root = tree.getroot()

ns = {'s': 'http://maven.apache.org/SETTINGS/1.0.0'}
ET.register_namespace('', 'http://maven.apache.org/SETTINGS/1.0.0')
ET.register_namespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')

settings = root.find('s:profiles', ns) or root.find('profiles')
if settings is None:
  settings = ET.SubElement(root, 'profiles')

profile = ET.SubElement(settings, 'profile')
ET.SubElement(profile, 'id').text = 'ci-offline-releases'

repos = ET.SubElement(profile, 'repositories')
repo = ET.SubElement(repos, 'repository')
ET.SubElement(repo, 'id').text = 'central'
ET.SubElement(repo, 'url').text = 'https://repo.maven.apache.org/maven2'
releases = ET.SubElement(repo, 'releases')
ET.SubElement(releases, 'enabled').text = 'true'
ET.SubElement(releases, 'updatePolicy').text = 'never'
snapshots = ET.SubElement(repo, 'snapshots')
ET.SubElement(snapshots, 'enabled').text = 'false'

prepos = ET.SubElement(profile, 'pluginRepositories')
prepo = ET.SubElement(prepos, 'pluginRepository')
ET.SubElement(prepo, 'id').text = 'central'
ET.SubElement(prepo, 'url').text = 'https://repo.maven.apache.org/maven2'
preleases = ET.SubElement(prepo, 'releases')
ET.SubElement(preleases, 'enabled').text = 'true'
ET.SubElement(preleases, 'updatePolicy').text = 'never'
psnapshots = ET.SubElement(prepo, 'snapshots')
ET.SubElement(psnapshots, 'enabled').text = 'false'

active = root.find('s:activeProfiles', ns) or root.find('activeProfiles')
if active is None:
  active = ET.SubElement(root, 'activeProfiles')

ET.SubElement(active, 'activeProfile').text = 'ci-offline-releases'

tree.write(settings_file, encoding='UTF-8', xml_declaration=True)
