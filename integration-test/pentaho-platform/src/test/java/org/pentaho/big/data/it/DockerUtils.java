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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Thin wrapper around the local {@code docker} CLI so integration tests can run commands inside the
 * PDI / Hadoop / HBase containers and copy files in and out, without depending on any EE-only helper
 * library. The container ids are provided by the docker-maven-plugin through failsafe system properties.
 */
public final class DockerUtils {

  /** Result of a command executed inside a container. */
  public static final class ExecResult {
    private final int exitCode;
    private final String stdout;
    private final String stderr;

    ExecResult( int exitCode, String stdout, String stderr ) {
      this.exitCode = exitCode;
      this.stdout = stdout;
      this.stderr = stderr;
    }

    public int exitCode() {
      return exitCode;
    }

    public String stdout() {
      return stdout;
    }

    public String stderr() {
      return stderr;
    }

    public boolean isSuccess() {
      return exitCode == 0;
    }

    public String combinedOutput() {
      return stdout + ( stderr.isBlank() ? "" : System.lineSeparator() + stderr );
    }

    @Override
    public String toString() {
      return "ExecResult{exitCode=" + exitCode + ", stdout=" + stdout + ", stderr=" + stderr + '}';
    }
  }

  private DockerUtils() {
  }

  /** Runs a command inside a container as the default user. */
  public static ExecResult exec( String containerId, String... command ) throws IOException, InterruptedException {
    return execAsUser( containerId, null, command );
  }

  /** Runs a command inside a container, optionally as a specific user (e.g. {@code root}). */
  public static ExecResult execAsUser( String containerId, String user, String... command )
    throws IOException, InterruptedException {
    List<String> cmd = new ArrayList<>();
    cmd.add( "docker" );
    cmd.add( "exec" );
    if ( user != null && !user.isBlank() ) {
      cmd.add( "-u" );
      cmd.add( user );
    }
    cmd.add( containerId );
    for ( String c : command ) {
      cmd.add( c );
    }
    return run( cmd );
  }

  /**
   * Runs a shell pipeline inside a container. Useful for interactive tools that read from stdin
   * (for example {@code echo "scan 'table'" | hbase shell}).
   */
  public static ExecResult execShell( String containerId, String shellScript )
    throws IOException, InterruptedException {
    return exec( containerId, "/bin/bash", "-lc", shellScript );
  }

  /** Copies a file or directory from a container to the host. */
  public static void copyFromContainer( String containerId, String containerPath, Path hostPath )
    throws IOException, InterruptedException {
    Files.createDirectories( hostPath.getParent() );
    ExecResult r = run( List.of( "docker", "cp", containerId + ":" + containerPath, hostPath.toString() ) );
    if ( !r.isSuccess() ) {
      throw new IOException( "docker cp from container failed: " + r.combinedOutput() );
    }
  }

  /** Copies a file or directory from the host into a container. */
  public static void copyToContainer( String containerId, Path hostPath, String containerPath )
    throws IOException, InterruptedException {
    ExecResult r = run( List.of( "docker", "cp", hostPath.toString(), containerId + ":" + containerPath ) );
    if ( !r.isSuccess() ) {
      throw new IOException( "docker cp to container failed: " + r.combinedOutput() );
    }
  }

  /** Reads the full contents of a file inside a container. */
  public static String readFile( String containerId, String containerPath )
    throws IOException, InterruptedException {
    ExecResult r = exec( containerId, "cat", containerPath );
    if ( !r.isSuccess() ) {
      throw new IOException( "Could not read " + containerPath + " from container: " + r.combinedOutput() );
    }
    return r.stdout();
  }

  private static ExecResult run( List<String> command ) throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder( command );
    Process process = pb.start();

    // Drain stdout and stderr concurrently. A single-threaded read of one stream fully before the
    // other deadlocks when the child fills the other pipe buffer (~64 KB) - which PAN easily does
    // through its logging - leaving both the child (blocked on write) and this thread (blocked on
    // read) stuck forever, so the waitFor timeout below would never even be reached.
    CompletableFuture<byte[]> outFuture = readStreamAsync( process.getInputStream() );
    CompletableFuture<byte[]> errFuture = readStreamAsync( process.getErrorStream() );

    if ( !process.waitFor( 5, TimeUnit.MINUTES ) ) {
      process.destroyForcibly();
      throw new IOException( "Command timed out: " + String.join( " ", command ) );
    }

    String stdout = new String( outFuture.join(), StandardCharsets.UTF_8 );
    String stderr = new String( errFuture.join(), StandardCharsets.UTF_8 );
    return new ExecResult( process.exitValue(), stdout, stderr );
  }

  /** Reads a process stream to completion on a separate thread so both pipes can drain in parallel. */
  private static CompletableFuture<byte[]> readStreamAsync( java.io.InputStream stream ) {
    return CompletableFuture.supplyAsync( () -> {
      try ( stream ) {
        return stream.readAllBytes();
      } catch ( IOException e ) {
        throw new UncheckedIOException( e );
      }
    } );
  }
}
