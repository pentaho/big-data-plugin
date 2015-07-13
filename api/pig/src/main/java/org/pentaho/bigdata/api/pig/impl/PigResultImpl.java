package org.pentaho.bigdata.api.pig.impl;

import org.apache.commons.vfs.FileObject;
import org.pentaho.bigdata.api.pig.PigResult;

/**
 * Created by bryan on 7/9/15.
 */
public class PigResultImpl implements PigResult {
  private final FileObject logFile;
  private final int[] result;
  private final Exception exception;

  public PigResultImpl( FileObject logFile, int[] result, Exception exception ) {
    this.logFile = logFile;
    this.result = result;
    this.exception = exception;
  }

  @Override public FileObject getLogFile() {
    return logFile;
  }

  @Override public int[] getResult() {
    return result;
  }

  @Override public Exception getException() {
    return exception;
  }
}
