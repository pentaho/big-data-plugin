/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.big.data.hadoop.bootstrap;

import org.pentaho.hadoop.shim.HadoopConfiguration;

/**
 * Created by bryan on 6/8/15.
 */
public interface HadoopConfigurationListener {
  void onClassLoaderAvailable( ClassLoader classLoader );

  void onConfigurationOpen( HadoopConfiguration hadoopConfiguration, boolean defaultConfiguration );

  void onConfigurationClose( HadoopConfiguration hadoopConfiguration );
}
