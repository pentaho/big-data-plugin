package org.pentaho.bigdata.api.pig;

import org.apache.commons.vfs.FileObject;

/**
 * Created by bryan on 7/9/15.
 */
public interface PigResult {
  FileObject getLogFile();
  int[] getResult();
  Exception getException();
}
