Big Data Plugin in 6.1
======================
OSGi
----
As of 6.1, all the main Hadoop functionality (HDFS, MapReduce, PMR, HBase, Pig, Sqoop, Oozie, YARN) is accesible via OSGi services.

HDFS, Pig, and YARN were moved over as part of the 6.0 release so this doesn't introduce a new paradigm so much as complete the migration.

This should not currently impact the user experience (shims, config files are in the same place, it is just the steps and services themselves that are now in OSGi)

This does allow for any OSGi plugin to leverage OSGi services in the future (no longer limited to the Big Data Plugin)

It also paves the way for the eventual addition of more advanced authentication/authorization as well as multi-shim support.

Files Being Moved/Modified
--------------------------
The individual Kettle Plugins from the old Big Data Plugin have been split into an API that exposes shim capability as a series of OSGi services, an implementation using the shim, and the Kettle Plugin that consumes the API.  The shims themselves and the configuration of the Big Data Plugin have not changed for this release.

Affected Products
-----------------
This affects all parts of the stack capable of using the Kettle Big Data Plugin steps and job entries.

License Impact
--------------
There should be no change in licensing driven by these changes

Deployment Impact
-----------------
Updates to the legacy Big Data Plugin should be the same as before, drop a new plugins/big-data-plugin folder in and configure it (or just unzip a new shim into the hadoop-configurations directory)

Updates to the OSGi bundles can be accomplished most easily by building the same version as the release and overwriting the bundle in the Karaf system repository.  After this, stop the tool, remove the Karaf cache, and start the tool back up.
