/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.bigdata.api.hbase;

import org.pentaho.bigdata.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.bigdata.api.hbase.mapping.MappingFactory;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;

import java.io.IOException;

/**
 * Created by bryan on 1/19/16.
 */
public interface HBaseService {
  HBaseConnection getHBaseConnection( VariableSpace variableSpace, String siteConfig, String defaultConfig, LogChannelInterface logChannelInterface )
    throws IOException;

  ColumnFilterFactory getColumnFilterFactory();

  MappingFactory getMappingFactory();

  HBaseValueMetaInterfaceFactory getHBaseValueMetaInterfaceFactory();

  ByteConversionUtil getByteConversionUtil();

  ResultFactory getResultFactory();
}
