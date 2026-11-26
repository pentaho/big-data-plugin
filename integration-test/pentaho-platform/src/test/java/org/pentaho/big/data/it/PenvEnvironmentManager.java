/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.it;

import org.apache.commons.io.FileUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cross-platform Maven lifecycle bridge that delegates integration-test container setup and
 * teardown to {@code penv} without relying on host-side shell scripts.
 * <p>
 * Bound to {@code pre-integration-test} ({@code up}) and {@code post-integration-test} ({@code down})
 * in {@code integration-test/pentaho-platform/pom.xml}.
 */
public final class PenvEnvironmentManager {

  private static final String PROP_SKIP_PENV = "bigdata.it.skip-penv";
  private static final String PROP_PENV_BIN = "bigdata.it.penv-bin";
  private static final String PROP_PENV_WORKSPACE = "bigdata.it.penv-workspace";
  private static final String PROP_PENV_STATE_FILE = "bigdata.it.penv-state-file";
  private static final String PROP_PENV_TEMPLATE_DIR = "bigdata.it.penv-template-dir";
  private static final String PROP_DOCKER_DIR = "bigdata.it.docker-dir";
  private static final String PROP_PLUGIN_UNPACK_DIR = "bigdata.it.plugin-unpack-dir";
  private static final String PROP_PLUGIN_DIR_NAME = "bigdata.it.plugin-directory-name";
  private static final String PROP_TEST_OUTPUT_DIR = "bigdata.it.test-output-dir";
  private static final String PROP_TEST_RESOURCES_DIR = "bigdata.it.test-resources-dir";
  private static final String PROP_METASTORE_DIR = "bigdata.it.metastore-dir";
  private static final String PROP_KETTLE_DIR = "bigdata.it.kettle-dir";
  private static final String PROP_PLATFORM = "bigdata.it.platform";
  private static final String PROP_START_TIMEOUT = "bigdata.it.penv-start-timeout";

  private static final String MOUNT_PROBE_IMAGE = "eclipse-temurin:21-jdk-jammy";
  private static final Pattern STATE_PROJECT_PATTERN = Pattern.compile( "\"project\"\\s*:\\s*\"([^\"]+)\"" );

  /** Environment variables rejected by {@code penv_runtime.process_env} when pre-set in the caller shell. */
  private static final Set<String> PENV_RESERVED_ENV = Set.of(
    "COMPOSE_PROJECT_NAME",
    "REPRO_PROJECT_NAME",
    "COMPOSE_FILE",
    "COMPOSE_ENV_FILES",
    "PENV_INTERNAL_DIR",
    "PENV_ENGINE_DIR",
    "PENV_SERVER_DOWNLOADS_DIR",
    "PENV_PDI_DOWNLOADS_DIR",
    "PENV_SERVER_OVERLAY_DIR",
    "PENV_PDI_OVERLAY_DIR",
    "PENV_ENVIRONMENT_ID",
    "DOCKER_PLATFORM",
    "PENV_PLATFORM_SUFFIX"
  );

  private PenvEnvironmentManager() {
  }

  public static void main( String[] args ) throws Exception {
    if ( args.length != 1 || ( !"up".equals( args[ 0 ] ) && !"down".equals( args[ 0 ] ) ) ) {
      throw new IllegalArgumentException( "Usage: PenvEnvironmentManager <up|down>" );
    }
    if ( Boolean.parseBoolean( System.getProperty( PROP_SKIP_PENV, "false" ) ) ) {
      System.out.println( "[penv] Skipping penv lifecycle (" + PROP_SKIP_PENV + "=true)." );
      return;
    }

    if ( "up".equals( args[ 0 ] ) ) {
      up();
    } else {
      down();
    }
  }

  private static void up() throws Exception {
    List<String> penvCmd = resolvePenvCommand();
    Path workspace = resolveWorkspaceForUp();
    Path stateFile = requiredPath( PROP_PENV_STATE_FILE );
    Path containerPropsFile = requiredPath( ITUtils.PROP_CONTAINER_PROPERTIES_FILE );

    Files.createDirectories( workspace );
    Files.createDirectories( stateFile.getParent() );
    Files.writeString( stateFile, workspace.toAbsolutePath().toString(), StandardCharsets.UTF_8 );

    try {
      String platform = System.getProperty( PROP_PLATFORM, "linux/amd64" );
      verifyDockerWorkspaceVisibility( workspace, platform );
      stageWorkspace( workspace );

      int timeoutSeconds = Integer.parseInt( System.getProperty( PROP_START_TIMEOUT, "600" ) );
      System.out.println( "[penv] Starting PDI and Big Data fixture services in " + workspace );
      runStreaming( buildPenvArgs( penvCmd,
        "--no-color", "start", "-w", workspace.toString(), "--timeout", String.valueOf( timeoutSeconds ), "pdi" ),
        30 );

      String project = readComposeProjectName( workspace );
      String pdiId = requireRunningServiceContainer( project, "pdi" );
      String hadoopId = requireRunningServiceContainer( project, "hadoop" );
      String hbaseId = requireRunningServiceContainer( project, "hbase" );
      String postgresId = requireRunningServiceContainer( project, "sqoop-postgres" );

      stagePdiRuntimeResources( pdiId );
      writeContainerProperties( containerPropsFile, pdiId, hadoopId, hbaseId, postgresId );

      System.out.println( "[penv] Environment ready (project=" + project + ", pdi=" + shortId( pdiId )
        + ", hadoop=" + shortId( hadoopId ) + ", hbase=" + shortId( hbaseId )
        + ", postgres=" + shortId( postgresId ) + ")." );
    } catch ( Throwable failure ) {
      System.err.println( "[penv] Startup failed; collecting diagnostics and cleaning up workspace " + workspace );
      dumpFailureDiagnosticsQuietly( penvCmd, workspace );
      destroyWorkspaceQuietly( penvCmd, workspace, stateFile );
      throw failure;
    }
  }

  private static void down() throws Exception {
    Path stateFile = optionalPath( PROP_PENV_STATE_FILE );
    Path workspace = null;
    if ( stateFile != null && Files.isRegularFile( stateFile ) ) {
      String recorded = Files.readString( stateFile, StandardCharsets.UTF_8 ).trim();
      if ( !recorded.isEmpty() ) {
        workspace = Path.of( recorded );
      }
    }
    if ( workspace == null ) {
      workspace = optionalPath( PROP_PENV_WORKSPACE );
    }
    if ( workspace == null || !Files.exists( workspace ) ) {
      deleteIfExistsQuietly( stateFile );
      System.out.println( "[penv] No active workspace found to tear down." );
      return;
    }

    List<String> penvCmd = resolvePenvCommand();
    System.out.println( "[penv] Destroying penv environment in " + workspace );
    if ( Files.isRegularFile( workspace.resolve( "penv.json" ) )
      || Files.isRegularFile( workspace.resolve( ".penv" ).resolve( "state.json" ) ) ) {
      runStreaming( buildPenvArgs( penvCmd, "--no-color", "destroy", "-w", workspace.toString(), "--yes" ), 10 );
    }
    FileUtils.deleteDirectory( workspace.toFile() );
    deleteIfExistsQuietly( stateFile );
  }

  private static Path resolveWorkspaceForUp() {
    Path configured = requiredPath( PROP_PENV_WORKSPACE ).toAbsolutePath().normalize();
    String envRoot = System.getenv( "BIGDATA_IT_WORKSPACE_ROOT" );
    if ( envRoot != null && !envRoot.isBlank() ) {
      return Path.of( envRoot.trim() ).toAbsolutePath().normalize().resolve( configured.getFileName() );
    }
    return configured;
  }

  private static void verifyDockerWorkspaceVisibility( Path workspace, String platform )
    throws IOException, InterruptedException {
    String token = "penv-mount-ok-" + UUID.randomUUID();
    Path probeFile = workspace.resolve( ".mount-probe" );
    Files.writeString( probeFile, token, StandardCharsets.UTF_8 );
    try {
      List<String> cmd = List.of(
        "docker", "run", "--rm",
        "--platform", platform,
        "--network", "none",
        "--mount", "type=bind,src=" + workspace.toAbsolutePath() + ",dst=/penv-mount-probe,readonly",
        MOUNT_PROBE_IMAGE,
        "cat", "/penv-mount-probe/.mount-probe"
      );
      DockerUtils.ExecResult res = runCaptured( cmd, 5 );
      if ( !res.isSuccess() || !token.equals( res.stdout().trim() ) ) {
        throw new IllegalStateException(
          "Docker daemon cannot read bind-mounted workspace path '" + workspace.toAbsolutePath() + "'. "
            + "Set -Dbigdata.it.penv-workspace-root=<shared-path> or BIGDATA_IT_WORKSPACE_ROOT=<shared-path> "
            + "to a directory shared with the Docker daemon. Details: " + res.combinedOutput() );
      }
    } finally {
      deleteIfExistsQuietly( probeFile );
    }
  }

  private static void stageWorkspace( Path workspace ) throws IOException {
    Path templateDir = requiredPath( PROP_PENV_TEMPLATE_DIR );
    Path dockerDir = requiredPath( PROP_DOCKER_DIR );
    Path pluginUnpackDir = requiredPath( PROP_PLUGIN_UNPACK_DIR );
    String pluginDirName = System.getProperty( PROP_PLUGIN_DIR_NAME, "pentaho-big-data-plugin" );

    Files.copy( templateDir.resolve( "penv.json" ), workspace.resolve( "penv.json" ),
      StandardCopyOption.REPLACE_EXISTING );
    Files.copy( templateDir.resolve( "compose.yaml" ), workspace.resolve( "compose.yaml" ),
      StandardCopyOption.REPLACE_EXISTING );
    Files.writeString( workspace.resolve( "penv.config.json" ),
      "{\n  \"licenseMode\": \"manual\"\n}\n", StandardCharsets.UTF_8 );

    copyDirectory( dockerDir.resolve( "hadoop" ), workspace.resolve( "docker" ).resolve( "hadoop" ) );
    copyDirectory( dockerDir.resolve( "postgres" ), workspace.resolve( "docker" ).resolve( "postgres" ) );

    Path testOutputDir = optionalPath( PROP_TEST_OUTPUT_DIR );
    if ( testOutputDir != null ) {
      copyDirectoryIfPresent( testOutputDir.resolve( "transformations" ),
        workspace.resolve( "overrides" ).resolve( "pdi" ).resolve( "transformations" ) );
      copyDirectoryIfPresent( testOutputDir.resolve( "jobs" ),
        workspace.resolve( "overrides" ).resolve( "pdi" ).resolve( "jobs" ) );
    }

    Path sourcePluginDir = pluginUnpackDir.resolve( pluginDirName );
    if ( !Files.isDirectory( sourcePluginDir )
      || ( !Files.isRegularFile( sourcePluginDir.resolve( "plugin.properties" ) )
        && !Files.isRegularFile( sourcePluginDir.resolve( "version.xml" ) )
        && !Files.isRegularFile( sourcePluginDir.resolve( "plugin.xml" ) ) ) ) {
      throw new IllegalStateException( "Unpacked Big Data plugin not found at " + sourcePluginDir
        + " (missing plugin.properties/version.xml). Ensure the plugin ZIP was built and unpacked before starting penv." );
    }
    Path targetPluginDir = workspace.resolve( "plugin" ).resolve( pluginDirName );
    Files.createDirectories( targetPluginDir.getParent() );
    moveOrCopyDirectory( sourcePluginDir, targetPluginDir );
    deleteDirectoryIfEmptyQuietly( pluginUnpackDir );
  }

  private static void stagePdiRuntimeResources( String pdiId ) throws IOException, InterruptedException {
    waitForPdiRunnerReady( pdiId );

    DockerUtils.ExecResult uidResult = DockerUtils.exec( pdiId, "id", "-u" );
    DockerUtils.ExecResult gidResult = DockerUtils.exec( pdiId, "id", "-g" );
    DockerUtils.ExecResult homeResult = DockerUtils.exec( pdiId, "printenv", "HOME" );
    if ( !uidResult.isSuccess() || !gidResult.isSuccess() || !homeResult.isSuccess() ) {
      throw new IOException( "Failed to inspect PDI runtime user: "
        + uidResult.combinedOutput() + gidResult.combinedOutput() + homeResult.combinedOutput() );
    }
    String uid = uidResult.stdout().trim();
    String gid = gidResult.stdout().trim();
    String home = homeResult.stdout().trim();
    if ( uid.isEmpty() || gid.isEmpty() || home.isEmpty() ) {
      throw new IOException( "Unexpected empty PDI user identity: uid='" + uid + "' gid='" + gid + "' home='" + home + "'" );
    }
    String containerMetastore = home + "/.pentaho/metastore";
    String containerKettle = home + "/.kettle";

    requireExecSuccess( DockerUtils.exec( pdiId, "mkdir", "-p", containerMetastore, containerKettle ),
      "Failed to create .pentaho/metastore and .kettle directories in PDI container" );

    copyDirContentsToContainerIfPresent( optionalPath( PROP_METASTORE_DIR ), pdiId, containerMetastore );
    Path testResourcesDir = optionalPath( PROP_TEST_RESOURCES_DIR );
    if ( testResourcesDir != null ) {
      copyDirContentsToContainerIfPresent( testResourcesDir.resolve( "metastore" ), pdiId, containerMetastore );
    }
    copyDirContentsToContainerIfPresent( optionalPath( PROP_KETTLE_DIR ), pdiId, containerKettle );

    requireExecSuccess( DockerUtils.execAsUser( pdiId, "root",
        "chown", "-R", uid + ":" + gid, home + "/.pentaho", containerKettle ),
      "Failed to set ownership on .pentaho and .kettle in PDI container" );

    // Remove RedshiftJDBC*.jar so its shaded SLF4J 1.7 does not trigger a LinkageError with the Big Data shim.
    requireExecSuccess( DockerUtils.exec( pdiId,
        "sh", "-c", "rm -f " + ITUtils.DATA_INTEGRATION_DIR + "/lib/RedshiftJDBC*.jar" ),
      "Failed to remove conflicting RedshiftJDBC jar from PDI container" );
  }

  private static void copyDirContentsToContainerIfPresent( Path hostDir, String containerId, String containerTargetDir )
    throws IOException, InterruptedException {
    if ( hostDir == null || !Files.isDirectory( hostDir ) ) {
      return;
    }
    DockerUtils.ExecResult cp = runCaptured(
      List.of( "docker", "cp", hostDir.toAbsolutePath() + File.separator + ".", containerId + ":" + containerTargetDir ),
      5 );
    if ( !cp.isSuccess() ) {
      throw new IOException( "Failed to copy " + hostDir + " into " + containerTargetDir + ": " + cp.combinedOutput() );
    }
  }

  private static String readComposeProjectName( Path workspace ) throws IOException {
    Path stateJson = workspace.resolve( ".penv" ).resolve( "state.json" );
    if ( !Files.isRegularFile( stateJson ) ) {
      throw new IOException( "Missing penv state file after startup: " + stateJson );
    }
    String json = Files.readString( stateJson, StandardCharsets.UTF_8 );
    Matcher matcher = STATE_PROJECT_PATTERN.matcher( json );
    if ( !matcher.find() ) {
      throw new IOException( "Could not find 'project' field in " + stateJson );
    }
    return matcher.group( 1 );
  }

  private static String requireRunningServiceContainer( String project, String service )
    throws IOException, InterruptedException {
    DockerUtils.ExecResult res = runCaptured( List.of(
      "docker", "ps", "--quiet", "--no-trunc",
      "--filter", "label=com.docker.compose.project=" + project,
      "--filter", "label=com.docker.compose.service=" + service,
      "--filter", "status=running"
    ), 2 );
    if ( !res.isSuccess() ) {
      throw new IOException( "Failed to query running container for service '" + service + "': " + res.combinedOutput() );
    }
    List<String> ids = new ArrayList<>();
    for ( String line : res.stdout().split( "\\R" ) ) {
      if ( !line.isBlank() ) {
        ids.add( line.trim() );
      }
    }
    if ( ids.size() != 1 ) {
      throw new IllegalStateException( "Expected exactly 1 running container for service '" + service
        + "' in project '" + project + "', found " + ids.size() + ": " + ids );
    }
    return ids.get( 0 );
  }

  private static void writeContainerProperties( Path file, String pdiId, String hadoopId,
                                                String hbaseId, String postgresId ) throws IOException {
    Files.createDirectories( file.getParent() );
    Properties props = new Properties();
    props.setProperty( ITUtils.PROP_PDI_CONTAINER, pdiId );
    props.setProperty( ITUtils.PROP_HADOOP_CONTAINER, hadoopId );
    props.setProperty( ITUtils.PROP_HBASE_CONTAINER, hbaseId );
    props.setProperty( ITUtils.PROP_POSTGRES_CONTAINER, postgresId );
    try ( OutputStream out = Files.newOutputStream( file ) ) {
      props.store( out, "Generated by PenvEnvironmentManager" );
    }
  }

  private static List<String> resolvePenvCommand() {
    Path penvExecutable = locatePenvExecutable();
    if ( isWindows() ) {
      String lowerName = penvExecutable.getFileName().toString().toLowerCase( Locale.ROOT );
      if ( lowerName.endsWith( ".cmd" ) || lowerName.endsWith( ".bat" ) ) {
        // Prefer invoking penv's managed Python + penv.py directly to avoid cmd.exe batch-quoting edge cases.
        Path installDir = penvExecutable.getParent() != null ? penvExecutable.getParent().getParent() : null;
        if ( installDir != null ) {
          Path venvPython = installDir.resolve( ".venv" ).resolve( "Scripts" ).resolve( "python.exe" );
          Path standalonePython = installDir.resolve( ".python" ).resolve( "python.exe" );
          Path penvPy = installDir.resolve( "penv.py" );
          if ( Files.isRegularFile( penvPy ) ) {
            if ( Files.isRegularFile( venvPython ) ) {
              return List.of( venvPython.toAbsolutePath().toString(), penvPy.toAbsolutePath().toString() );
            }
            if ( Files.isRegularFile( standalonePython ) ) {
              return List.of( standalonePython.toAbsolutePath().toString(), penvPy.toAbsolutePath().toString() );
            }
          }
        }
        return List.of( "cmd.exe", "/d", "/c", penvExecutable.toAbsolutePath().toString() );
      }
    }
    return List.of( penvExecutable.toAbsolutePath().toString() );
  }

  private static Path locatePenvExecutable() {
    String explicit = System.getProperty( PROP_PENV_BIN );
    if ( explicit == null || explicit.isBlank() ) {
      explicit = System.getenv( "PENV_BIN" );
    }
    if ( explicit != null && !explicit.isBlank() ) {
      Path candidate = Path.of( explicit.trim() );
      if ( Files.isRegularFile( candidate ) ) {
        return candidate;
      }
      throw new IllegalStateException( "Configured penv binary does not exist: " + candidate );
    }

    String[] names = isWindows()
      ? new String[] { "penv.cmd", "penv.exe", "penv.bat" }
      : new String[] { "penv" };
    String pathEnv = System.getenv( "PATH" );
    if ( pathEnv != null ) {
      for ( String entry : pathEnv.split( Pattern.quote( File.pathSeparator ) ) ) {
        if ( entry == null || entry.isBlank() ) {
          continue;
        }
        Path dir = Path.of( entry.trim() );
        for ( String name : names ) {
          Path candidate = dir.resolve( name );
          if ( Files.isRegularFile( candidate ) ) {
            return candidate;
          }
        }
      }
    }

    List<Path> standardLocations = new ArrayList<>();
    if ( isWindows() ) {
      String localAppData = System.getenv( "LOCALAPPDATA" );
      if ( localAppData != null && !localAppData.isBlank() ) {
        standardLocations.add( Path.of( localAppData, "Programs", "penv", "bin", "penv.cmd" ) );
      }
      String userHome = System.getProperty( "user.home" );
      if ( userHome != null && !userHome.isBlank() ) {
        standardLocations.add( Path.of( userHome, "AppData", "Local", "Programs", "penv", "bin", "penv.cmd" ) );
      }
    } else {
      String userHome = System.getProperty( "user.home" );
      if ( userHome != null && !userHome.isBlank() ) {
        standardLocations.add( Path.of( userHome, ".local", "bin", "penv" ) );
      }
    }
    for ( Path candidate : standardLocations ) {
      if ( Files.isRegularFile( candidate ) ) {
        return candidate;
      }
    }

    throw new IllegalStateException(
      "Could not find 'penv' executable. Install penv from pentaho/eng-sandbox "
        + "(Linux/macOS: ./penv/install.sh --local --ci; Windows: .\\penv\\install.ps1 -Local -NoPrompt) "
        + "or pass -D" + PROP_PENV_BIN + "=<path-to-penv>." );
  }

  private static List<String> buildPenvArgs( List<String> penvPrefix, String... args ) {
    List<String> cmd = new ArrayList<>( penvPrefix );
    for ( String arg : args ) {
      cmd.add( arg );
    }
    return cmd;
  }

  private static void dumpFailureDiagnosticsQuietly( List<String> penvCmd, Path workspace ) {
    try {
      if ( Files.isRegularFile( workspace.resolve( ".penv" ).resolve( "state.json" ) ) ) {
        String project = readComposeProjectName( workspace );
        DockerUtils.ExecResult ps = runCaptured( List.of(
          "docker", "ps", "-a", "--filter", "label=com.docker.compose.project=" + project ), 2 );
        System.err.println( ps.combinedOutput() );
        runStreaming( buildPenvArgs( penvCmd, "--no-color", "logs", "-w", workspace.toString(), "--tail", "80" ), 3 );
      }
    } catch ( Exception ignored ) {
      // Best-effort diagnostics only; do not mask the original startup exception.
    }
  }

  private static void destroyWorkspaceQuietly( List<String> penvCmd, Path workspace, Path stateFile ) {
    try {
      if ( Files.isRegularFile( workspace.resolve( ".penv" ).resolve( "state.json" ) ) ) {
        runStreaming( buildPenvArgs( penvCmd, "--no-color", "destroy", "-w", workspace.toString(), "--yes" ), 10 );
      }
    } catch ( Exception e ) {
      System.err.println( "[penv] Warning: cleanup after startup failure encountered an error: " + e.getMessage() );
    } finally {
      try {
        FileUtils.deleteDirectory( workspace.toFile() );
      } catch ( IOException ignored ) {
        // Ignore best-effort directory cleanup failure.
      }
      deleteIfExistsQuietly( stateFile );
    }
  }

  private static void runStreaming( List<String> command, long timeoutMinutes )
    throws IOException, InterruptedException {
    DockerUtils.ExecResult result = executeProcess( command, true, timeoutMinutes );
    if ( !result.isSuccess() ) {
      throw new IOException( "Command failed (exit " + result.exitCode() + "): " + String.join( " ", command ) );
    }
  }

  private static DockerUtils.ExecResult runCaptured( List<String> command, long timeoutMinutes )
    throws IOException, InterruptedException {
    return executeProcess( command, false, timeoutMinutes );
  }

  private static DockerUtils.ExecResult executeProcess( List<String> command, boolean streamOutput, long timeoutMinutes )
    throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder( command );
    Map<String, String> env = pb.environment();
    for ( String reserved : PENV_RESERVED_ENV ) {
      env.remove( reserved );
    }
    env.put( "PYTHONUTF8", "1" );
    env.put( "PYTHONIOENCODING", "utf-8" );

    Process process = pb.start();
    CompletableFuture<byte[]> outFuture = drainAsync( process.getInputStream(), streamOutput ? System.out : null );
    CompletableFuture<byte[]> errFuture = drainAsync( process.getErrorStream(), streamOutput ? System.err : null );

    if ( !process.waitFor( timeoutMinutes, TimeUnit.MINUTES ) ) {
      process.destroyForcibly();
      throw new IOException( "Command timed out after " + timeoutMinutes + " minutes: " + String.join( " ", command ) );
    }
    return new DockerUtils.ExecResult(
      process.exitValue(),
      new String( outFuture.join(), StandardCharsets.UTF_8 ),
      new String( errFuture.join(), StandardCharsets.UTF_8 ) );
  }

  private static CompletableFuture<byte[]> drainAsync( InputStream stream, PrintStream mirror ) {
    return CompletableFuture.supplyAsync( () -> {
      try ( stream ) {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        byte[] buffer = new byte[ 8192 ];
        int read;
        while ( ( read = stream.read( buffer ) ) != -1 ) {
          captured.write( buffer, 0, read );
          if ( mirror != null ) {
            mirror.write( buffer, 0, read );
            mirror.flush();
          }
        }
        return captured.toByteArray();
      } catch ( IOException e ) {
        throw new UncheckedIOException( e );
      }
    } );
  }

  private static void moveOrCopyDirectory( Path sourceDir, Path targetDir ) throws IOException {
    if ( Files.exists( targetDir ) ) {
      FileUtils.deleteDirectory( targetDir.toFile() );
    }
    try {
      Files.move( sourceDir, targetDir, StandardCopyOption.REPLACE_EXISTING );
    } catch ( IOException moveFailure ) {
      FileUtils.copyDirectory( sourceDir.toFile(), targetDir.toFile() );
    }
  }

  private static void waitForPdiRunnerReady( String pdiId ) throws IOException, InterruptedException {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos( 60 );
    String lastState = "";
    while ( System.nanoTime() < deadlineNanos ) {
      DockerUtils.ExecResult probe = DockerUtils.exec( pdiId, "cat", "/pentaho-init/state" );
      if ( probe.isSuccess() ) {
        lastState = probe.stdout().trim();
        if ( "ready".equals( lastState ) ) {
          return;
        }
        if ( lastState.startsWith( "failed" ) ) {
          throw new IOException( "PDI container initialization failed: " + lastState );
        }
      }
      Thread.sleep( 500L );
    }
    throw new IOException( "Timed out waiting for PDI container /pentaho-init/state to become 'ready' (last='"
      + lastState + "')" );
  }

  private static void deleteDirectoryIfEmptyQuietly( Path dir ) {
    if ( dir == null || !Files.isDirectory( dir ) ) {
      return;
    }
    try ( var entries = Files.list( dir ) ) {
      if ( entries.findFirst().isEmpty() ) {
        Files.deleteIfExists( dir );
      }
    } catch ( IOException ignored ) {
      // Best-effort cleanup of empty staging parent directory.
    }
  }

  private static void copyDirectoryIfPresent( Path sourceDir, Path targetDir ) throws IOException {
    if ( Files.isDirectory( sourceDir ) ) {
      copyDirectory( sourceDir, targetDir );
    }
  }

  private static void copyDirectory( Path sourceDir, Path targetDir ) throws IOException {
    if ( !Files.isDirectory( sourceDir ) ) {
      throw new IllegalStateException( "Required directory does not exist: " + sourceDir );
    }
    FileUtils.copyDirectory( sourceDir.toFile(), targetDir.toFile() );
  }

  private static void requireExecSuccess( DockerUtils.ExecResult result, String context ) throws IOException {
    if ( !result.isSuccess() ) {
      throw new IOException( context + ": " + result.combinedOutput() );
    }
  }

  private static Path requiredPath( String property ) {
    Path path = optionalPath( property );
    if ( path == null ) {
      throw new IllegalStateException( "Missing required system property: " + property );
    }
    return path;
  }

  private static Path optionalPath( String property ) {
    String value = System.getProperty( property );
    return ( value == null || value.isBlank() ) ? null : Path.of( value.trim() );
  }

  private static void deleteIfExistsQuietly( Path path ) {
    if ( path != null ) {
      try {
        Files.deleteIfExists( path );
      } catch ( IOException ignored ) {
        // Best-effort cleanup.
      }
    }
  }

  private static boolean isWindows() {
    return System.getProperty( "os.name", "" ).toLowerCase( Locale.ROOT ).contains( "win" );
  }

  private static String shortId( String id ) {
    return id.length() > 12 ? id.substring( 0, 12 ) : id;
  }
}
