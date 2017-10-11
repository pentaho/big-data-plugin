/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.job.entries.spark;

import com.sun.jna.Platform;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


public class WinProcessTest {

  private static String processCmdResource;
  private static String javaFileResource;
  private static String classPath;
  private static String childProcessClassName = "ChildProcessTester";

  @BeforeClass
  public static void setUp() {
    Assume.assumeTrue( Platform.isWindows() );

    processCmdResource =
      Thread.currentThread().getContextClassLoader().getResource( "process.cmd" ).getPath().toString().substring( 1 );
    javaFileResource =
      Thread.currentThread().getContextClassLoader().getResource( childProcessClassName + ".java" ).getPath().toString()
        .substring( 1 );
    int index = javaFileResource.lastIndexOf( '/' );
    classPath = javaFileResource.substring( 0, index );
  }

  @Test
  public void getPIDWhenProcessIsNull() {
    int pid = WinProcess.getPID( null );
    Assert.assertEquals( -1, pid );
  }

  @Test
  public void getPIDWhenProcessExists() {
    int pid = -1;
    Process process = null;
    List<String> cmds = new ArrayList<>();
    cmds.add( "cmd.exe" );
    ProcessBuilder processBuilder = new ProcessBuilder( cmds );
    try {
      process = processBuilder.start();
      pid = WinProcess.getPID( process );
    } catch ( IOException e ) {
      e.printStackTrace();
    } finally {
      process.destroy();
    }

    Assert.assertTrue( pid > 0 );
    Assert.assertFalse( process.isAlive() );
  }

  @Test
  public void createWinProcessWrapperAndKillProcess() {
    int pid;
    Process process = null;
    WinProcess winProcess = null;
    List<String> cmds = new ArrayList<>();
    cmds.add( "cmd.exe" );
    ProcessBuilder processBuilder = new ProcessBuilder( cmds );
    try {
      process = processBuilder.start();
      winProcess = new WinProcess( WinProcess.getPID( process ) );
      pid = winProcess.getWinProcessPID();

      Assert.assertNotNull( winProcess );
      Assert.assertTrue( pid > 0 );

    } catch ( IOException e ) {
      e.printStackTrace();
    } finally {
      winProcess.terminate();
    }

    Assert.assertFalse( process.isAlive() );
  }

  @Test
  public void tryToKillChildProcessIfItNotExists() {
    String childProcessPIDs = " ";
    Process process = null;
    WinProcess winProcess = null;
    List<String> cmds = new ArrayList<>();
    cmds.add( "cmd.exe" );
    ProcessBuilder processBuilder = new ProcessBuilder( cmds );
    try {
      process = processBuilder.start();
      winProcess = new WinProcess( WinProcess.getPID( process ) );
      childProcessPIDs = winProcess.killChildProcesses();
    } catch ( IOException e ) {
      e.printStackTrace();
    } finally {
      process.destroy();
    }
    Assert.assertEquals( childProcessPIDs, "" );
    Assert.assertTrue( childProcessPIDs.isEmpty() );
    Assert.assertFalse( process.isAlive() );
  }

  @Test
  public void killExistingChildProcess() {

    int parentPID;
    boolean isChildProcessExists;
    WinProcess winProcess;
    String childProcessPIDs = " ";
    String pattern = "child process started";

    List<String> cmds = new ArrayList<>();

    cmds.add( "cmd.exe" );
    cmds.add( "/c" );
    cmds.add( processCmdResource );
    cmds.add( javaFileResource );
    cmds.add( classPath );
    cmds.add( childProcessClassName );

    ProcessBuilder builder = new ProcessBuilder( cmds );
    try {
      Process proc = builder.start();

      InputStream inStream = proc.getInputStream();
      InputStream errStream = proc.getErrorStream();

      Thread inStreamReader = new Thread(
        () -> {
          try {
            String line = null;
            BufferedReader in = new BufferedReader( new InputStreamReader( inStream ) );
            while ( ( line = in.readLine() ) != null ) {
              System.out.println( line );
              if ( line.contains( pattern ) ) {
                proc.destroy();
              }
            }
          } catch ( IOException e ) {
            e.printStackTrace();
          }
        } );

      inStreamReader.start();

      Thread errStreamReader = new Thread(
        () -> {
          try {
            String line = null;
            BufferedReader in = new BufferedReader( new InputStreamReader( errStream ) );
            while ( ( line = in.readLine() ) != null ) {
              System.out.println( line );
              if ( line.contains( pattern ) ) {
                proc.destroy();
              }
            }
          } catch ( IOException e ) {
            e.printStackTrace();
          }
        } );

      errStreamReader.start();

      proc.waitFor();

      parentPID = WinProcess.getPID( proc );
      winProcess = new WinProcess( parentPID );
      childProcessPIDs = winProcess.killChildProcesses();
      isChildProcessExists = isChildProcessAlive( parentPID, childProcessPIDs );

      if ( isChildProcessExists ) {
        winProcess = new WinProcess( new Integer( childProcessPIDs ) );
        winProcess.terminate();
      }

      Assert.assertNotEquals( childProcessPIDs, "" );
      Assert.assertTrue( new Integer( childProcessPIDs ) > 0 );
      Assert.assertFalse( childProcessPIDs.isEmpty() );
      Assert.assertFalse( isChildProcessExists );

      proc.getErrorStream().close();
      proc.getInputStream().close();

    } catch ( IOException e ) {
      e.printStackTrace();
    } catch ( InterruptedException e ) {
      e.printStackTrace();
    }
  }

  private static boolean isChildProcessAlive( Integer parentPID, String childPID ) {

    List<String> cmds = new ArrayList<>();
    final AtomicBoolean childProcessExists = new AtomicBoolean( false );

    cmds.add( "wmic.exe" );
    cmds.add( "process" );
    cmds.add( "where" );
    cmds.add( "parentProcessId=" + parentPID );
    cmds.add( "get" );
    cmds.add( "processId" );

    ProcessBuilder builder = new ProcessBuilder( cmds );

    try {
      Process proc = builder.start();

      InputStream inStream = proc.getInputStream();
      InputStream errStream = proc.getErrorStream();

      Thread inStreamReader = new Thread(
        () -> {
          try {
            String line = null;
            BufferedReader in = new BufferedReader( new InputStreamReader( inStream ) );
            while ( ( line = in.readLine() ) != null ) {
              System.out.println( line );
              if ( line.contains( childPID ) ) {
                childProcessExists.set( true );
                proc.destroy();
              }
            }
          } catch ( IOException e ) {
            e.printStackTrace();
          }
        } );

      inStreamReader.start();

      Thread errStreamReader = new Thread(
        () -> {
          try {
            String line = null;
            BufferedReader in = new BufferedReader( new InputStreamReader( errStream ) );
            while ( ( line = in.readLine() ) != null ) {
              System.out.println( line );
              if ( line.contains( childPID ) ) {
                childProcessExists.set( true );
                proc.destroy();
              }
            }
          } catch ( IOException e ) {
            e.printStackTrace();
          }
        } );

      errStreamReader.start();

      proc.waitFor();

      proc.getErrorStream().close();
      proc.getInputStream().close();

    } catch ( IOException e ) {
      e.printStackTrace();
    } catch ( InterruptedException e ) {
      e.printStackTrace();
    }
    return childProcessExists.get();
  }
}
