#!/usr/bin/env python3
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

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
