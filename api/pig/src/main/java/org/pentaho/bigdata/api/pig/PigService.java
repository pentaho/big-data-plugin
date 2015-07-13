package org.pentaho.bigdata.api.pig;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.variables.VariableSpace;

import java.util.List;

/**
 * Created by bryan on 6/18/15.
 */
public interface PigService {
  boolean isLocalExecutionSupported();

  PigResult executeScript( String scriptPath, ExecutionMode executionMode, List<String> parameters, String name,
                       LogChannelInterface logChannelInterface, VariableSpace variableSpace, LogLevel logLevel );

  enum ExecutionMode {
    LOCAL, MAPREDUCE
  }
}
