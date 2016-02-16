Big Data Plugin in 6.1
======================
OSGi
----
As of 6.1, all the main Hadoop functionality (HDFS, MapReduce, PMR, HBase, Pig, Sqoop, Oozie, YARN) is accessible via OSGi services.

HDFS, Pig, and YARN were moved to OSGi services in the 6.0 version of the software.  For 6.1, MapReduce, PMR, HBase, Oozie, and Sqoop were moved to OSGi services. This doesn't introduce a new paradigm, it just completes the migration of the functionality to OSGi.

This won't impact the user experience.  Shims, configuration files are in the same place and all saved jobs and transformations will continue to work.  However, the steps and services themselves are now in OSGi.

This change allows any OSGi plugin to leverage OSGi services in the future; these services are no longer limited to the Big Data Plugin.

It also paves the way for the eventual addition of more advanced authentication/authorization as well as multi-shim support.

JDBC
----
Hive and Impala Drivers have not been migrated to OSGi, and are still part of the Big Data Plugin.

Files Being Moved/Modified
--------------------------
The individual Kettle Plugins from the old Big Data Plugin have been split into an API that exposes shim capability as a series of OSGi services, an implementation using the shim, and the Kettle Plugin that consumes the API.  The shims themselves and the configuration of the Big Data Plugin have not changed for this release.

Affected Products
-----------------
This affects all parts of the stack capable of using the Kettle Big Data Plugin steps and job entries.

License Impact
--------------
There should be no change in licensing driven by these changes.  Kerberos support and the YARN service and job entries remain EE features while the rest is still open.

Deployment Impact
-----------------
Updates to the legacy Big Data Plugin should be the same as before. Either drop a new big-data-plugin folder into the plugins directory and configure it, or unzip a new shim in the hadoop-configurations directory.

Updates to the OSGi bundles currently can be accomplished most easily by building the same version as the release and overwriting the bundle in the Karaf system repository.  After this, stop the tool, remove the Karaf cache, and restart the tool. The bundle updating process will be improved after 6.1 and we will be aiming for a much easier deployment scenario.
