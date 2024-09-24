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

package org.pentaho.big.data.kettle.plugins.hbase.output;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.big.data.kettle.plugins.hbase.NamedClusterLoadSaveUtil;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

public class HBaseOutputMetaInjectionTest extends BaseMetadataInjectionTest<HBaseOutputMeta> {

  @Before
  public void setup() {
    NamedClusterService namedClusterService = Mockito.mock( NamedClusterService.class );
    NamedClusterServiceLocator namedClusterServiceLocator = Mockito.mock( NamedClusterServiceLocator.class );
    RuntimeTestActionService runtimeTestActionService = Mockito.mock( RuntimeTestActionService.class );
    RuntimeTester runtimeTester = Mockito.mock( RuntimeTester.class );
    MetastoreLocator metaStore = Mockito.mock( MetastoreLocator.class );

    setup( new HBaseOutputMeta( namedClusterService, namedClusterServiceLocator, runtimeTestActionService, runtimeTester, new NamedClusterLoadSaveUtil(),
      metaStore ) );
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
