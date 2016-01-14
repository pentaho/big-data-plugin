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

package org.pentaho.bigdata.api.mapreduce;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.IOException;
import java.net.URL;

/**
 * Interface for creating MapReduce jobs
 */
public interface MapReduceService {
  /**
   * Executes the main method in a jar that is responsible to submitting a MapReduce job
   *
   * @param resolvedJarUrl  the jar url
   * @param driverClass     the main class
   * @param commandLineArgs command line arguments
   * @return a MapReduceJobSimple reference for tracking progress
   * @throws MapReduceExecutionException
   */
  MapReduceJobSimple executeSimple( URL resolvedJarUrl, String driverClass, String commandLineArgs )
    throws MapReduceExecutionException;

  /**
   * Returns a MapReduceJobBuilder for configuring and launching a more complex MapReduce job
   *
   * @param log           the log
   * @param variableSpace the variable space
   * @return a MapReduceJobBuilder
   */
  MapReduceJobBuilder createJobBuilder( LogChannelInterface log, VariableSpace variableSpace );

  PentahoMapReduceJobBuilder createPentahoMapReduceJobBuilder( LogChannelInterface log, VariableSpace variableSpace )
    throws IOException;

  /**
   * Returns relevant information on a jar
   *
   * @param resolvedJarUrl the jar url
   * @return relevant information on a jar
   * @throws IOException
   * @throws ClassNotFoundException
   */
  MapReduceJarInfo getJarInfo( URL resolvedJarUrl ) throws IOException, ClassNotFoundException;

  /**
   * Interface for clients to implement if they would like to terminate the wait functionality before the timeout
   */
  interface Stoppable {
    /**
     * Returns a boolean indicating whether the parent process is stopped
     *
     * @return a boolean indicating whether the parent process is stopped
     */
    boolean isStopped();
  }
}
