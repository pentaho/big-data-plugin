/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hbase.output;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

public class HBaseOutputMetaInjectionTest extends BaseMetadataInjectionTest<HBaseOutputMeta> {

  @Before
  public void setup() {
    NamedClusterService namedClusterService = Mockito.mock( NamedClusterService.class );
    NamedClusterServiceLocator namedClusterServiceLocator = Mockito.mock( NamedClusterServiceLocator.class );
    RuntimeTestActionService runtimeTestActionService = Mockito.mock( RuntimeTestActionService.class );
    RuntimeTester runtimeTester = Mockito.mock( RuntimeTester.class );

    setup( new HBaseOutputMeta( namedClusterService, namedClusterServiceLocator, runtimeTestActionService, runtimeTester ) );
  }

  @Test
  public void test() throws Exception {
    check( "HBASE_SITE_XML_URL", new StringGetter() {
      public String get() {
        return meta.getCoreConfigURL();
      }
    } );
    check( "HBASE_DEFAULT_XML_URL", new StringGetter() {
      public String get() {
        return meta.getDefaultConfigURL();
      }
    } );
    check( "TARGET_TABLE_NAME", new StringGetter() {
      public String get() {
        return meta.getTargetTableName();
      }
    } );
    check( "TARGET_MAPPING_NAME", new StringGetter() {
      public String get() {
        return meta.getTargetMappingName();
      }
    } );
    check( "DELETE_ROW_KEY", new BooleanGetter()  {
      @Override
      public boolean get() {
        return meta.getDeleteRowKey();
      }
    } );
    check( "DISABLE_WRITE_TO_WAL", new BooleanGetter() {
      public boolean get() {
        return meta.getDisableWriteToWAL();
      }
    } );
    check( "WRITE_BUFFER_SIZE", new StringGetter() {
      public String get() {
        return meta.getWriteBufferSize();
      }
    } );

    check( "TABLE_NAME", new StringGetter() {
      public String get() {
        return meta.getMappingDefinition().getTableName();
      }
    } );
    check( "MAPPING_NAME", new StringGetter() {
      public String get() {
        return meta.getMappingDefinition().getMappingName();
      }
    } );

    check( "MAPPING_ALIAS", new StringGetter() {
      public String get() {
        return meta.getMappingDefinition().getMappingColumns().get( 0 ).getAlias();
      }
    } );
    check( "MAPPING_KEY", new BooleanGetter() {
      public boolean get() {
        return meta.getMappingDefinition().getMappingColumns().get( 0 ).isKey();
      }
    } );
    check( "MAPPING_COLUMN_FAMILY", new StringGetter() {
      public String get() {
        return meta.getMappingDefinition().getMappingColumns().get( 0 ).getColumnFamily();
      }
    } );
    check( "MAPPING_COLUMN_NAME", new StringGetter() {
      public String get() {
        return meta.getMappingDefinition().getMappingColumns().get( 0 ).getColumnName();
      }
    } );
    check( "MAPPING_TYPE", new StringGetter() {
      public String get() {
        return meta.getMappingDefinition().getMappingColumns().get( 0 ).getType();
      }
    } );
    check( "MAPPING_INDEXED_VALUES", new StringGetter() {
      public String get() {
        return meta.getMappingDefinition().getMappingColumns().get( 0 ).getIndexedValues();
      }
    } );
  }
  
}
