# PDI Integration-Test License Setup

## Why the tests failed

The integration tests run `pan.sh` and `kitchen.sh` inside the
`one.hitachivantara.com/pnt-docker/automation/pdi-client:11.1` image. That image is
an EE PDI client and validates a Pentaho ELM license before it starts a
transformation or job.

The original Docker build copied the plugin, transformations, jobs, and
metastore into the image, but it did not copy a license. The PDI launcher showed
this diagnostic:

```text
DEBUG: PENTAHO_LICENSE_INFORMATION_PATH=
ERROR [elm] ERROR: License response status: The current license server URL has expired
Pan - No valid license found.
```

The failure occurs before HDFS, YARN, PostgreSQL, or HBase work begins. That is
why `BasicIT`, `HdfsIT`, and `SqoopIT` all failed with the same PAN or Kitchen
license error.

`LICENSE.txt` in this repository is the source license for the code. It is not
a runtime PDI license and must not be used as the value of
`bigdata.it.license-file`.

The integration-test fixture keeps its runtime license at
`integration-test/conf/.elmLicInfo.plt`. This project-local file is the default
input for the Docker image, so the test does not depend on a license file in the
invoking user's home directory.

## Supported license file

PDI's launcher looks for an ELM license information file named
`.elmLicInfo.plt`. For the test image's installation, the launcher checks these
locations, in order:

```text
/home/devuser/pentaho/design-tools/data-integration/.elmLicInfo.plt
/home/devuser/pentaho/design-tools/.elmLicInfo.plt
/home/devuser/pentaho/.elmLicInfo.plt
```

The fixture uses the third location. When it finds the file, `pan.sh` and
`kitchen.sh` pass the following JVM property to the PDI process:

```text
-Dpentaho.license.information.file=/home/devuser/pentaho/.elmLicInfo.plt
```

The file must contain a valid, non-expired Pentaho ELM license configuration.
A URL that points to an expired license server is not sufficient; obtain or
renew the license through the organization's normal Pentaho licensing process.
The PDI distribution also contains the supported installer at
`design-tools/license-installer/install_license.sh` (or the Windows `.bat`
variant). Its help describes the accepted activation ID, license-server URL,
and `hostid` forms.

## Repository behavior

`integration-test/pentaho-platform/pom.xml` now does the following during
`pre-integration-test`:

1. Uses `bigdata.it.license-file` as the source path. The default is
  `integration-test/conf/.elmLicInfo.plt`.
2. Deletes and recreates `target/license` so a previous run cannot leave a
   different license in the Docker build context.
3. Fails before Docker startup if the selected file does not exist.
4. Copies the selected file to `target/license/.elmLicInfo.plt`.
5. Adds that staged file to the PDI image at `/home/devuser/pentaho/.elmLicInfo.plt`.

The license is therefore available to the container even when the Docker daemon
is remote or running in Docker-in-Docker. The file is sent as part of the
Docker build context; it is not bind-mounted from the host at container runtime.
The project-local source remains under `integration-test/conf`; only the staged
copy under `target` is disposable. Treat the license file as sensitive and
protect access to it according to the organization's licensing policy.

## Run the tests

The project-local license is the default, so no license path option is required:

```powershell
mvn.cmd -o -pl integration-test/pentaho-platform verify `
  '-Dpentaho.docker.pull.host=one.hitachivantara.com/pnt-docker/' `
  -DrunIntegrationTests `
  -Papachevanilla
```

To choose a different valid license file, override the property. Use forward
slashes or quote the complete `-D` argument in PowerShell:

```powershell
mvn.cmd -o -pl integration-test/pentaho-platform verify `
  '-Dpentaho.docker.pull.host=one.hitachivantara.com/pnt-docker/' `
  '-Dbigdata.it.license-file=C:/path/to/valid/.elmLicInfo.plt' `
  -DrunIntegrationTests `
  -Papachevanilla
```

For a faster focused run, select one Failsafe test and stop at the
`integration-test` goal. Containers are intentionally left running by that
goal, so remove only the test-owned containers afterward if the run is
interrupted:

```powershell
mvn.cmd -o -T 4 `
  '-Dbigdata.it.license-file=C:/path/to/valid/.elmLicInfo.plt' `
  -Dtest=NoSuchTest `
  -DfailIfNoTests=false `
  -DrunIntegrationTests `
  '-Dit.test=HdfsIT' `
  '-Dbigdata.it.plugin-variant=apachevanilla' `
  '-Dpentaho.docker.pull.host=one.hitachivantara.com/pnt-docker/' `
  -pl integration-test/pentaho-platform `
  integration-test

docker ps -aq --filter "name=^/bigdata-it-" | ForEach-Object { docker rm -f $_ }
```

Use the reactor form with `-am` when the plugin ZIP itself has changed. The
license staging behavior is the same:

```powershell
mvn.cmd -o -T 4 `
  -Papachevanilla `
  '-Dbigdata.it.license-file=C:/path/to/valid/.elmLicInfo.plt' `
  -Dtest=NoSuchTest `
  -DfailIfNoTests=false `
  -DrunIntegrationTests `
  '-Dit.test=HdfsIT' `
  '-Dpentaho.docker.pull.host=one.hitachivantara.com/pnt-docker/' `
  -pl integration-test/pentaho-platform `
  -am `
  integration-test
```

## CI and shared runners

A CI runner uses the valid `.elmLicInfo.plt` under `integration-test/conf` by
default. The CI checkout therefore needs access to that project configuration,
and the Docker build copies it only into the test image. Do not publish the
staged file under `target` or include it in a plugin ZIP or other runtime
artifact.

When a runner needs a different valid license, pass its path with
`-Dbigdata.it.license-file=...` rather than changing the POM.

If the runner uses a remote Docker daemon, the path must be readable by the
Maven process that creates the Docker build context. A path that exists only on
the Docker daemon host will not work. The build-context approach used here
handles that boundary by transferring the selected file during `docker build`.

If several CI jobs share a license file, verify that the license permits the
number of concurrent PDI processes. A valid file can still be rejected by the
license service when its activation, server URL, host identity, or concurrency
entitlement is no longer valid.

## Troubleshooting

### `PENTAHO_LICENSE_INFORMATION_PATH=` is empty

The file was not copied into the PDI image at one of the launcher discovery
locations. Check that:

```powershell
Test-Path 'integration-test/conf/.elmLicInfo.plt'
Test-Path 'integration-test/pentaho-platform/target/license/.elmLicInfo.plt'
```

Then rebuild the PDI image. The target file is the copy that the Docker assembly
uses; changing the source file without rerunning Maven does not change an
already-built image.

### `The current license server URL has expired`

The file reached PDI, but the ELM configuration is no longer valid. This is not
an HDFS or Docker-network error. Renew the license or select another valid file:

```powershell
mvn.cmd -o -pl integration-test/pentaho-platform verify `
  '-Dbigdata.it.license-file=C:/path/to/valid/.elmLicInfo.plt' `
  -DrunIntegrationTests `
  -Papachevanilla
```

### The test still uses an old license

The `pre-integration-test` setup reads `integration-test/conf/.elmLicInfo.plt`,
deletes and recreates `target/license`, and the PDI image build copies the fresh
staged file. If a diagnostic run was stopped before teardown, remove only
containers whose names start with `bigdata-it-` and rerun Maven so the fixture
image and container are recreated.

### Maven fails before PAN starts

A missing license now fails early with a message that includes the selected
`bigdata.it.license-file` path. This is intentional: it avoids spending time
starting four containers when PAN and Kitchen cannot run.

## Evidence for this fix

The original failure reports showed an empty `PENTAHO_LICENSE_INFORMATION_PATH`
and `No valid license found` from `TransStartExtensionPoint`. The existing local
`.elmLicInfo.plt` was then mounted at `/home/devuser/pentaho/.elmLicInfo.plt` in
the derived PDI image. The same `basic_smoke.ktr` run printed:

```text
DEBUG: Found Pentaho license information two folders up
DEBUG: PENTAHO_LICENSE_INFORMATION_PATH=/home/devuser/pentaho/design-tools/data-integration/../../.elmLicInfo.plt
basic_smoke - License validated.
```

That proves the file format and the PDI discovery path are valid. The Maven
change makes that proven container setup reproducible from the integration-test
project's `conf` directory instead of relying on the invoking user's home.
