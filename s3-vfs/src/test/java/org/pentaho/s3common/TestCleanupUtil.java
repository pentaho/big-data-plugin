package org.pentaho.s3common;

import java.io.File;

public class TestCleanupUtil {
  private TestCleanupUtil() { }

  public static void cleanUpLogsDir() {
    File logsDir = new File( "logs" );
    if ( logsDir.exists() && logsDir.isDirectory() ) {
      File[] files = logsDir.listFiles();
      if ( files != null ) {
        for ( File f : files ) {
          f.delete();
        }
      }
      logsDir.delete();
    }
  }
}
