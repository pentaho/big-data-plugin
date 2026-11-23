# Big Data plugin integration tests

The opt-in suite runs PDI with the built plugin against disposable Hadoop, PostgreSQL and HBase
containers. `SqoopIT` imports a PostgreSQL table into HDFS and checks the Kitchen log for Sqoop
counters. HDFS and basic plugin tests also run; the unfinished HBase and format tests are disabled.

## Prerequisites

- JDK 11 or later, Maven, and a Docker daemon with sufficient available memory.
- Access to the Pentaho Maven and Docker registries and the `automation/pdi-client` image.
- An approved, valid PDI ELM license information file. Do not commit the license.
- Linux amd64 container support; the draft images currently require that platform.

## Run

From the repository root, build the plugin and run the focused test:

```sh
mvn -pl integration-test/pentaho-platform -am verify \
  -DrunIntegrationTests -Papachevanilla \
  -Dtest=NoSuchTest -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=SqoopIT \
  -Dbigdata.it.license-file=/path/to/.elmLicInfo.plt \
  -Dpentaho.docker.pull.host=<registry>/
```

`-Dtest=NoSuchTest` avoids running unrelated Surefire unit tests in the reactor; the accompanying
flags allow that empty selection, while Failsafe selects `SqoopIT` separately. Omit
`-Dit.test=SqoopIT` to run the full integration
suite. Do not use `-DskipTests`, which may also skip Failsafe. Check
`integration-test/pentaho-platform/target/failsafe-reports` to confirm that tests actually ran.

On Docker Desktop, if the Maven plugin does not discover the current Docker context, supply
`-Dbigdata.it.dockerHost="$(docker context inspect --format '{{.Endpoints.docker.Host}}')"`.
For an already-cached PDI image, `-Dbigdata.it.pdi-pull-policy=IfNotPresent` avoids forcing a pull.

The PDI license is copied into the test container at runtime and removed after each test class;
it is not copied into a Docker image. Test classes sharing that container are serialized, including
their license setup and teardown. CI needs the `BIGDATA_IT_LICENSE_INFORMATION` secret set
through the organization's approved secret-management process.

The containers share a test network and do not publish their service ports to the host. The
network name is based on the Maven build timestamp; provide a distinct
`-Duse-existing-docker-network=<name>` only when deliberately using an existing isolated network.
Image tags are shared, so do not run multiple copies of the suite on the same daemon concurrently.
Do not run global Docker cleanup on a shared daemon.
