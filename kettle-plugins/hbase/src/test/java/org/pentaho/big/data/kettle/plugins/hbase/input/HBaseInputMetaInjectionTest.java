/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.big.data.kettle.plugins.hbase.input;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

public class HBaseInputMetaInjectionTest extends BaseMetadataInjectionTest<HBaseInputMeta> {

  @Before
  public void setup() {
    NamedClusterService namedClusterService = Mockito.mock( NamedClusterService.class );
    NamedClusterServiceLocator namedClusterServiceLocator = Mockito.mock( NamedClusterServiceLocator.class );
    RuntimeTestActionService runtimeTestActionService = Mockito.mock( RuntimeTestActionService.class );
    RuntimeTester runtimeTester = Mockito.mock( RuntimeTester.class );
    MetastoreLocatorOsgi metaStore = Mockito.mock( MetastoreLocatorOsgi.class );

    setup( new HBaseInputMeta( namedClusterService, namedClusterServiceLocator, runtimeTestActionService, runtimeTester, metaStore ) );
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
    check( "SOURCE_TABLE_NAME", new StringGetter() {
      public String get() {
        return meta.getSourceTableName();
      }
    } );
    check( "SOURCE_MAPPING_NAME", new StringGetter() {
      public String get() {
        return meta.getSourceMappingName();
      }
    } );
    check( "START_KEY_VALUE", new StringGetter() {
      public String get() {
        return meta.getKeyStartValue();
      }
    } );
    check( "STOP_KEY_VALUE", new StringGetter() {
      public String get() {
        return meta.getKeyStopValue();
      }
    } );
    check( "SCANNER_ROW_CACHE_SIZE", new StringGetter() {
      public String get() {
        return meta.getScannerCacheSize();
      }
    } );
    check( "MATCH_ANY_FILTER", new BooleanGetter() {
      public boolean get() {
        return meta.getMatchAnyFilter();
      }
    } );

    check( "OUTPUT_FIELD_KEY", new BooleanGetter() {
      public boolean get() {
        return meta.getOutputFieldsDefinition().get( 0 ).isKey();
      }
    } );
    check( "OUTPUT_FIELD_ALIAS", new StringGetter() {
      public String get() {
        return meta.getOutputFieldsDefinition().get( 0 ).getAlias();
      }
    } );
    check( "OUTPUT_FIELD_COLUMN_NAME", new StringGetter() {
      public String get() {
        return meta.getOutputFieldsDefinition().get( 0 ).getColumnName();
      }
    } );
    check( "OUTPUT_FIELD_FAMILY", new StringGetter() {
      public String get() {
        return meta.getOutputFieldsDefinition().get( 0 ).getFamily();
      }
    } );
    check( "OUTPUT_FIELD_TYPE", new StringGetter() {
      public String get() {
        return meta.getOutputFieldsDefinition().get( 0 ).getHbaseType();
      }
    } );
    check( "OUTPUT_FIELD_FORMAT", new StringGetter() {
      public String get() {
        return meta.getOutputFieldsDefinition().get( 0 ).getFormat();
      }
    } );

    check( "ALIAS", new StringGetter() {
      public String get() {
        return meta.getFiltersDefinition().get( 0 ).getAlias();
      }
    } );
    check( "FIELD_TYPE", new StringGetter() {
      public String get() {
        return meta.getFiltersDefinition().get( 0 ).getFieldType();
      }
    } );
    check( "SIGNED_COMPARISON", new BooleanGetter() {
      public boolean get() {
        return meta.getFiltersDefinition().get( 0 ).isSignedComparison();
      }
    } );
    check( "COMPARISON_VALUE", new StringGetter() {
      public String get() {
        return meta.getFiltersDefinition().get( 0 ).getConstant();
      }
    } );
    check( "FORMAT", new StringGetter() {
      public String get() {
        return meta.getFiltersDefinition().get( 0 ).getFormat();
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
    skipPropertyTest( "COMPARISON_TYPE" );
  }

}
