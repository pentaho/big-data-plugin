/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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
