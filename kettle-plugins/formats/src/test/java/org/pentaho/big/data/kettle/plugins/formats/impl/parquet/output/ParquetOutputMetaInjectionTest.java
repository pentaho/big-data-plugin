/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.di.core.row.value.ValueMetaBase;

public class ParquetOutputMetaInjectionTest extends BaseMetadataInjectionTest<ParquetOutputMeta> {

  @Before
  public void setup() {
    NamedClusterService namedClusterService = mock( NamedClusterService.class );
    NamedClusterServiceLocator namedClusterServiceLocator = mock( NamedClusterServiceLocator.class );
    MetastoreLocatorOsgi metaStoreService = mock( MetastoreLocatorOsgi.class );
    setup( new ParquetOutputMeta( namedClusterServiceLocator,
      namedClusterService, metaStoreService ) );
  }

  @Test
  public void test() throws Exception {
    check( "FILENAME", new StringGetter() {
      public String get() {
        return meta.getFilename();
      }
    } );

    check( "ROW_GROUP_SIZE", new StringGetter() {
      public String get() {
        return meta.getRowGroupSize();
      }
    } );
    check( "DATA_PAGE_SIZE", new StringGetter() {
      public String get() {
        return meta.getDataPageSize();
      }
    } );
    check( "ENABLE_DICTIONARY", new BooleanGetter() {
      public boolean get() {
        return meta.isEnableDictionary();
      }
    } );
    check( "DICT_PAGE_SIZE", new StringGetter() {
      public String get() {
        return meta.getDictPageSize();
      }
    } );
    check( "OVERRIDE_OUTPUT", new BooleanGetter() {
      public boolean get() {
        return meta.isOverrideOutput();
      }
    } );
    check( "INC_DATE_IN_FILENAME", new BooleanGetter() {
      public boolean get() {
        return meta.isDateInFilename();
      }
    } );
    check( "INC_TIME_IN_FILENAME", new BooleanGetter() {
      public boolean get() {
        return meta.isTimeInFilename();
      }
    } );
    check( "EXTENSION", new StringGetter() {
      public String get() {
        return meta.getExtension();
      }
    } );

    check( "DATE_FORMAT", new StringGetter() {
      public String get() {
        return meta.getDateTimeFormat();
      }
    } );

    check( "FIELD_NAME", new StringGetter() {
      public String get() {
        return meta.getOutputFields()[ 0 ].getName();
      }
    } );

    String[] typeNames = ValueMetaBase.getAllTypes();
    checkStringToInt( "FIELD_TYPE", new IntGetter() {
      public int get() {
        return meta.getOutputFields()[ 0 ].getType();
      }
    }, typeNames, getTypeCodes( typeNames ) );

    check( "FIELD_PATH", new StringGetter() {
      public String get() {
        return meta.getOutputFields()[ 0 ].getPath();
      }
    } );
    check( "FIELD_IF_NULL", new StringGetter() {
      public String get() {
        return meta.getOutputFields()[ 0 ].getIfNullValue();
      }
    } );
    check( "FIELD_NULLABLE", new BooleanGetter() {
      public boolean get() {
        return meta.getOutputFields()[ 0 ].isNullable();
      }
    } );
    checkStringToInt( "FIELD_SOURCE_TYPE", new IntGetter() {
      public int get() {
        return meta.getOutputFields()[ 0 ].getSourceType();
      }
    }, typeNames, getTypeCodes( typeNames ) );


    skipPropertyTest( "COMPRESSION" );
    skipPropertyTest( "PARQUET_VERSION" );
  }
}
