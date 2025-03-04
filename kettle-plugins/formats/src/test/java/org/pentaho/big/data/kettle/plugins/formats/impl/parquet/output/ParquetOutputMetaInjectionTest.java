/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.big.data.kettle.plugins.formats.parquet.ParquetTypeConverter;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;

public class ParquetOutputMetaInjectionTest extends BaseMetadataInjectionTest<ParquetOutputMeta> {

  @Before
  public void setup() {
    NamedClusterResolver namedClusterResolver = mock( NamedClusterResolver.class );
    setup( new ParquetOutputMeta( namedClusterResolver ) );
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
        return meta.getOutputFields().get( 0 ).getPentahoFieldName();
      }
    } );

    int [] supportedPdiTypes = {
      ValueMetaInterface.TYPE_NUMBER,
      ValueMetaInterface.TYPE_STRING,
      ValueMetaInterface.TYPE_DATE,
      ValueMetaInterface.TYPE_BOOLEAN,
      ValueMetaInterface.TYPE_INTEGER,
      ValueMetaInterface.TYPE_BIGNUMBER,
      ValueMetaInterface.TYPE_SERIALIZABLE,
      ValueMetaInterface.TYPE_BINARY,
      ValueMetaInterface.TYPE_TIMESTAMP,
      ValueMetaInterface.TYPE_INET
    };
    String[] typeNames = new String[ supportedPdiTypes.length ];
    int[] typeIds = new int[ supportedPdiTypes.length ];
    for ( int j = 0; j < supportedPdiTypes.length; j++ ) {
      typeNames[ j ] = ValueMetaInterface.getTypeDescription( supportedPdiTypes[ j ] );
      String parquetTypeName = ParquetTypeConverter.convertToParquetType( supportedPdiTypes[ j ] );
      for ( ParquetSpec.DataType parquetType : ParquetSpec.DataType.values() ) {
        if ( parquetType.getName().equals( parquetTypeName ) ) {
          typeIds[ j ] = parquetType.getId();
          break;
        }
      }
    }
    checkStringToInt( "FIELD_TYPE", new IntGetter() {
      public int get() {
        return meta.getOutputFields().get( 0 ).getFormatType();
      }
    }, typeNames, typeIds );


    ParquetSpec.DataType[] supportedParquetTypes = {
      ParquetSpec.DataType.UTF8,
      ParquetSpec.DataType.INT_32,
      ParquetSpec.DataType.INT_64,
      ParquetSpec.DataType.FLOAT,
      ParquetSpec.DataType.DOUBLE,
      ParquetSpec.DataType.BOOLEAN,
      ParquetSpec.DataType.DECIMAL,
      ParquetSpec.DataType.DATE,
      ParquetSpec.DataType.TIMESTAMP_MILLIS,
      ParquetSpec.DataType.BINARY
    };
    typeNames = new String[ supportedParquetTypes.length ];
    typeIds = new int[ supportedParquetTypes.length ];
    for ( int i = 0; i < supportedParquetTypes.length; i++ ) {
      typeNames[ i ] = supportedParquetTypes[ i ].getName();
      typeIds[ i ] = supportedParquetTypes[ i ].getId();
    }
    checkStringToInt( "FIELD_PARQUET_TYPE", new IntGetter() {
      public int get() {
        return meta.getOutputFields().get( 0 ).getFormatType();
      }
    }, typeNames, typeIds );

    check( "FIELD_DECIMAL_PRECISION", new IntGetter() {
      public int get() {
        return meta.getOutputFields().get( 0 ).getPrecision();
      }
    } );

    check( "FIELD_DECIMAL_SCALE", new IntGetter() {
      public int get() {
        return meta.getOutputFields().get( 0 ).getScale();
      }
    } );

    check( "FIELD_PATH", new StringGetter() {
      public String get() {
        return meta.getOutputFields().get( 0 ).getFormatFieldName();
      }
    } );

    check( "FIELD_IF_NULL", new StringGetter() {
      public String get() {
        return meta.getOutputFields().get( 0 ).getDefaultValue();
      }
    } );
    check( "FIELD_NULLABLE", new BooleanGetter() {
      public boolean get() {
        return meta.getOutputFields().get( 0 ).getAllowNull();
      }
    } );

    skipPropertyTest( "COMPRESSION" );
    skipPropertyTest( "PARQUET_VERSION" );
  }
}
