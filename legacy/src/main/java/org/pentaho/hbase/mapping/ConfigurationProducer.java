/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hbase.mapping;

import org.pentaho.hbase.shim.spi.HBaseConnection;

/**
 * Interface to something that can produce a connection to HBase
 * 
 * @author Mark Hall (mhall{[at]}penthao{[dot]}com)
 * @version $Revision$
 * 
 */
public interface ConfigurationProducer {

  /**
   * Get a configuration object encapsulating connection information for HBase
   * 
   * @return a HBaseConnection object for interacting with the currently configured connection to HBase
   * @throws Exception
   *           if the connection can't be supplied for some reason
   */
  HBaseConnection getHBaseConnection() throws Exception;

  String getCurrentConfiguration();
}
