/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.input;


import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.formats.parquet.input.ParquetInputField;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;

import static org.mockito.Mockito.mock;

public class ParquetInputMetaInjectionTest extends BaseMetadataInjectionTest<ParquetInputMeta> {

  @Before
  public void setup() {
    NamedClusterService namedClusterService = mock( NamedClusterService.class );
    NamedClusterServiceLocator namedClusterServiceLocator = mock( NamedClusterServiceLocator.class );
    MetastoreLocatorOsgi metaStoreLocator = mock( MetastoreLocatorOsgi.class );
    setup( new ParquetInputMeta( namedClusterServiceLocator,
      namedClusterService, metaStoreLocator ) );
  }

  @Test
  public void test() throws Exception {
    check( "FILENAME", new StringGetter() {
      public String get() {
        return meta.inputFiles.fileName[ 0 ];
      }
    } );

    check( "FIELD_NAME", new StringGetter() {
      public String get() {
        return meta.inputFields[ 0 ].getPentahoFieldName();
      }
    } );

    check( "IGNORE_EMPTY_FOLDER", new BooleanGetter() {
      public boolean get() {
        return meta.isIgnoreEmptyFolder();
      }
    } );


    String[] typeNames = ValueMetaBase.getAllTypes();
    checkStringToInt( "FIELD_TYPE", new IntGetter() {
      public int get() {
        return meta.inputFields[ 0 ].getPentahoType();
      }
    }, typeNames, getTypeCodes( typeNames ) );

    check( "FIELD_PATH", new StringGetter() {
      public String get() {
        return meta.inputFields[ 0 ].getFormatFieldName();
      }
    } );

    String[] parquetTypeNames = ParquetSpec.DataType.getDisplayableTypeNames();
    checkStringToInt( "PARQUET_TYPE", new IntGetter() {
      public int get() {
        return meta.inputFields[ 0 ].getParquetType().getId();
      }
    }, parquetTypeNames, getParquetTypeCodes( parquetTypeNames ) );
  }

  public static int[] getParquetTypeCodes( String[] parquetTypeNames ) {
    int[] parquetTypeCodes = new int[ parquetTypeNames.length ];

    for ( int i = 0; i < parquetTypeNames.length; ++i ) {
      ParquetInputField field = new ParquetInputField();
      field.setParquetType( parquetTypeNames[ i ] );
      parquetTypeCodes[ i ] = field.getParquetType().getId();
    }

    return parquetTypeCodes;
  }
}
