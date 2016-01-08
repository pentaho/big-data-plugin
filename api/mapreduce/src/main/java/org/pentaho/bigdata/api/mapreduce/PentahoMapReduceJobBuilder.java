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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;

/**
 * Created by bryan on 1/7/16.
 */
public interface PentahoMapReduceJobBuilder extends MapReduceJobBuilder {
  String getHadoopWritableCompatibleClassName( ValueMetaInterface valueMetaInterface );

  void setMapperInfo( String mapperTransformationXml, String mapperInputStep, String mapperOutputStep );

  void setCombinerInfo( String combinerTransformationXml, String combinerInputStep, String combinerOutputStep );

  void setReducerInfo( String reducerTransformationXml, String reducerInputStep, String reducerOutputStep );

  void setLogLevel( LogLevel logLevel );

  void setCleanOutputPath( boolean cleanOutputPath );

  void verifyTransMeta( TransMeta transMeta, String inputStepName, String outputStepName ) throws KettleException;
}
