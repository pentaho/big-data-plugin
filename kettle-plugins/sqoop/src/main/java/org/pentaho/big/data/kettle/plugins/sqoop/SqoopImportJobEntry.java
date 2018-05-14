/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
    documentationUrl = "http://wiki.pentaho.com/display/EAI/Sqoop+Import" )
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
