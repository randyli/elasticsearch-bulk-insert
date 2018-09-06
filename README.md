# elasticsearch-bulk-insert
This is an unofficial elasticsearch bulk insert plugin which support the newest version of Elasticsearch.
It's implemented by elastisearch low level rest api. 

It's tested in kettle 8.1 

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


License
-------
Licensed under the Apache License, Version 2.0. See LICENSE.txt for more information.
