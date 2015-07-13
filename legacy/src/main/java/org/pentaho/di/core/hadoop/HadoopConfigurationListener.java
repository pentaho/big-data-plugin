package org.pentaho.di.core.hadoop;

import org.pentaho.hadoop.shim.HadoopConfiguration;

/**
 * Created by bryan on 6/8/15.
 */
public interface HadoopConfigurationListener {
  void onConfigurationOpen( HadoopConfiguration hadoopConfiguration, boolean defaultConfiguration );

  void onConfigurationClose( HadoopConfiguration hadoopConfiguration );
}
