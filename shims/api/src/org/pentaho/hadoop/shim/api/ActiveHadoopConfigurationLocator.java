/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.api;

import org.pentaho.hadoop.shim.ConfigurationException;

/**
 * Provides a mechanism for a {@link HadoopConfigurationProvider} to locate the
 * active {@link HadoopConfiguration} by id.
 * 
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public interface ActiveHadoopConfigurationLocator {
  /**
   * @return the active Hadoop configuration's identifier
   * @throws ConfigurationException Error determining the currently active Hadoop configuration 
   */
  String getActiveConfigurationId() throws ConfigurationException;
}
