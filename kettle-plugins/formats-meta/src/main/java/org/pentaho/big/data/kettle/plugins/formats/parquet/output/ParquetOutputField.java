package org.pentaho.big.data.kettle.plugins.formats.parquet.output;

import org.pentaho.big.data.kettle.plugins.formats.BaseFormatOutputField;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.IParquetOutputField;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;

public class ParquetOutputField extends BaseFormatOutputField implements IParquetOutputField {

  @Override
  public ParquetSpec.DataType getParquetType() {
    for ( ParquetSpec.DataType type : ParquetSpec.DataType.values() ) {
      if ( type.getId() == formatType ) {
        return type;
      }
    }
    return null;
  }

  public void setFormatType( ParquetSpec.DataType formatType ) {
    this.formatType = formatType.getId();
  }

  @Injection( name = "FIELD_PARQUET_TYPE", group = "FIELDS" )
  public void setFormatType( String typeName ) {
    try  {
      setFormatType( Integer.parseInt( typeName ) );
    } catch ( NumberFormatException nfe ) {
      for ( ParquetSpec.DataType parquetType : ParquetSpec.DataType.values() ) {
        if ( parquetType.getName().equals( typeName ) ) {
          this.formatType = parquetType.getId();
        }
      }
    }
  }

  @Injection( name = "FIELD_TYPE", group = "FIELDS" )
  @Deprecated
  public void setPentahoType( String typeName ) {
    for ( int i = 0; i < ValueMetaInterface.typeCodes.length; i++ ) {
      if ( typeName.equals( ValueMetaInterface.typeCodes[ i ] ) ) {
        setFormatType( ParquetOutputMetaBase.convertToParquetType( i ) );
      }
    }
  }

}
