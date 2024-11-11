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


package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.input;


import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.big.data.kettle.plugins.formats.parquet.input.ParquetInputField;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;

import static org.mockito.Mockito.mock;

public class ParquetInputMetaInjectionTest extends BaseMetadataInjectionTest<ParquetInputMeta> {

  @Before
  public void setup() {
    NamedClusterResolver namedClusterResolver = mock( NamedClusterResolver.class );
    setup( new ParquetInputMeta( namedClusterResolver ) );
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
