/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;

import java.io.IOException;

/**
 * Interface to something that can produce a connection to HBase
 * 
 * @author Mark Hall (mhall{[at]}penthao{[dot]}com)
 * @version $Revision$
 * 
 */
public interface ConfigurationProducer {
  HBaseService getHBaseService() throws ClusterInitializationException;

  /**
   * Get a configuration object encapsulating connection information for HBase
   * 
   * @return a HBaseConnection object for interacting with the currently configured connection to HBase
   * @throws Exception
   *           if the connection can't be supplied for some reason
   */
  HBaseConnection getHBaseConnection() throws ClusterInitializationException, IOException;

  String getCurrentConfiguration();
}
