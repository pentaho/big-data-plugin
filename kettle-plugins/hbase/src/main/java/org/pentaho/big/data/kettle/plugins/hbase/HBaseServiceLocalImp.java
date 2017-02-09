/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.hbase;

import java.io.IOException;

import org.pentaho.bigdata.api.hbase.ByteConversionUtil;
import org.pentaho.bigdata.api.hbase.HBaseConnection;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.bigdata.api.hbase.ResultFactory;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.bigdata.api.hbase.mapping.MappingFactory;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;

import com.pentaho.big.data.bundles.impl.shim.hbase.ByteConversionUtilImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.mapping.ColumnFilterFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.mapping.MappingFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;

public class HBaseServiceLocalImp implements HBaseService {

  @Override
  public HBaseConnection getHBaseConnection( VariableSpace variableSpace, String siteConfig, String defaultConfig, LogChannelInterface logChannelInterface ) throws IOException {
    return null;
  }

  @Override
  public ColumnFilterFactory getColumnFilterFactory() {
    return new ColumnFilterFactoryImpl();
  }

  @Override
  public MappingFactory getMappingFactory() {
    return new MappingFactoryImpl( null, getHBaseValueMetaInterfaceFactory() );
  }

  @Override
  public HBaseValueMetaInterfaceFactoryImpl getHBaseValueMetaInterfaceFactory() {
    return new HBaseValueMetaInterfaceFactoryImpl( null );
  }

  @Override
  public ByteConversionUtil getByteConversionUtil() {
    return new ByteConversionUtilImpl( null );
  }

  @Override
  public ResultFactory getResultFactory() {
    return null;
  }

}
