/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.sqoop;

import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.util.Properties;

/**
 * Provides a way to orchestrate <a href="http://sqoop.apache.org/">Sqoop</a> imports.
 */
@JobEntry( id = "SqoopImport", name = "Sqoop.Import.PluginName", description = "Sqoop.Import.PluginDescription",
    categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData", image = "sqoop-import.svg",
    i18nPackageName = "org.pentaho.di.job.entries.sqoop", version = "1",
    documentationUrl = "https://pentaho-community.atlassian.net/wiki/display/EAI/Sqoop+Import" )
public class SqoopImportJobEntry extends AbstractSqoopJobEntry<SqoopImportConfig> {

  public SqoopImportJobEntry( NamedClusterService namedClusterService,
                              NamedClusterServiceLocator serviceLocator,
                              RuntimeTestActionService runtimeTestActionService,
                              RuntimeTester runtimeTester ) {
    super( namedClusterService, serviceLocator, runtimeTestActionService, runtimeTester );
  }

  @Override protected SqoopImportConfig createJobConfig() {
    return new SqoopImportConfig( this );
  }

  /**
   * @return the name of the Sqoop import tool: "import"
   */
  @Override
  protected String getToolName() {
    return "import";
  }

  @Override
  public void configure( SqoopImportConfig sqoopConfig, Properties properties ) throws KettleException {
    super.configure( sqoopConfig, properties );
    if ( sqoopConfig.getHbaseZookeeperQuorum() != null ) {
      properties.put( "hbase.zookeeper.quorum", environmentSubstitute( sqoopConfig.getHbaseZookeeperQuorum() ) );
    }
    if ( sqoopConfig.getHbaseZookeeperClientPort() != null ) {
      properties.put( "hbase.zookeeper.property.clientPort",
          environmentSubstitute( sqoopConfig.getHbaseZookeeperClientPort() ) );
    }
  }
}
