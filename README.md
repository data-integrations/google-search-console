# Search Console Batch Source

Description
-----------

Reads given dimensions from a Google Search Console api in JSON format. Outputs records with dimensions provided in config.

Use Case
--------

This source is used whenever you need to read from a Google Search Console.
Build
-----
To build your plugins:

    mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, if your artifact is named 'my-plugins-1.0.0':

    > load artifact target/my-plugins-1.0.0.jar config-file target/my-plugins-1.0.0.json
