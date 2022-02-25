Pentaho Big Data Plugin
=======================

The Pentaho Big Data Plugin Project provides support for an ever-expanding Big Data community within the Pentaho ecosystem. It is a plugin for the Pentaho Kettle engine which can be used within Pentaho Data Integration (Kettle), Pentaho Reporting, and the Pentaho BI Platform.

Building
--------
It's a maven build, so `mvn clean install` is a typical default for a local build.

Pre-requisites
---------------
JDK 8 in your path.
Maven 3.3.9 in your path.
This [settings.xml](https://raw.githubusercontent.com/pentaho/maven-parent-poms/master/maven-support-files/settings.xml)

How to use the custom settings.xml
---------------
Option 1: Copy this file into your <user-home>/.m2 folder and name it "settings.xml". 
Warning: If you do this, it will become your default settings.xml for all maven builds.

Option 2: Copy this file into some other folder--possibly the project folder for the project you want to build and use the maven 's' option to build with this settings.xml file. Example: `mvn -s public-settings.xml install`.

The Pentaho profile defaults to pull all artifacts through the Pentaho public repository. 
If you want to try resolving maven plugin dependencies through the maven central repository instead of the Pentaho public repository, activate the "central" profile like this:

`mvn -s -public-settings.xml -P central install`


If your fails to resolve the jacoco-maven-plugin version 0.7.7-SNAPSHOT
---------------
The 0.7.7-SNAPSHOT property version for the jacoco-maven-plugin is defined in several releases of the Pentaho parent poms, but it is only available in the Pentaho artifact repositories. If you are trying to resolve through maven central or other public repositories you should override to get the latest version like this:

`mvn -s -public-settings.xml -P central install -Djacoco-maven-plugin.version=0.7.7.201606060606`

Further Reading
---------------
Additional documentation is available on the Community wiki: [Big Data Plugin for Java Developers]( https://pentaho-community.atlassian.net/wiki/display/BAD/Getting+Started+for+Java+Developers)

License
-------
Licensed under the Apache License, Version 2.0. See LICENSE.txt for more information.
