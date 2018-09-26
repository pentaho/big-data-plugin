package org.pentaho.big.data.kettle.plugins.hbase.input;

import org.apache.hadoop.hbase.util.Bytes;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;

public class AELHBaseValueMetaImpl extends ValueMetaBase implements HBaseValueMetaInterface {
  private boolean isKey;
  private String alias;
  private String columnName;
  private String columnFamily;
  private String mappingName;
  private String tableName;
  private boolean isLongOrDouble = true;

  public AELHBaseValueMetaImpl( boolean isKey, String alias, String columnName, String columnFamily, String mappingName, String tableName ) {
    super( alias );
    this.isKey = isKey;
    this.alias = alias;
    this.columnName = columnName;
    this.columnFamily = columnFamily;
    this.mappingName = mappingName;
    this.tableName = tableName;
  }

  @Override
  public boolean isKey() {
    return isKey;
  }

  @Override
  public void setKey( boolean key ) {
    isKey = key;
  }

  @Override
  public String getAlias() {
    return getName();
  }

  @Override
  public void setAlias( String alias ) {
    this.alias = alias;
    setName( alias );
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public void setColumnName( String columnName ) {
    this.columnName = columnName;
  }

  @Override
  public String getColumnFamily() {
    return columnFamily;
  }

  @Override
  public void setColumnFamily( String columnFamily ) {
    this.columnFamily = columnFamily;
  }

  @Override
  public void setHBaseTypeFromString( String hbaseType ) throws IllegalArgumentException {
    if ( hbaseType.equalsIgnoreCase( "Integer" ) ) {
      setType( ValueMeta.getType( hbaseType ) );
      setIsLongOrDouble( false );
      return;
    }
    if ( hbaseType.equalsIgnoreCase( "Long" ) ) {
      setType( ValueMeta.getType( "Integer" ) );
      setIsLongOrDouble( true );
      return;
    }
    if ( hbaseType.equals( "Float" ) ) {
      setType( ValueMeta.getType( "Number" ) );
      setIsLongOrDouble( false );
      return;
    }
    if ( hbaseType.equals( "Double" ) ) {
      setType( ValueMeta.getType( "Number" ) );
      setIsLongOrDouble( true );
      return;
    }

    // default
    int type = ValueMeta.getType( hbaseType );
    if ( type == ValueMetaInterface.TYPE_NONE ) {
      throw new IllegalArgumentException( BaseMessages.getString( PKG,
          "HBaseValueMeta.Error.UnknownType", hbaseType ) );
    }

    setType( type );
  }

  @Override
  public String getHBaseTypeDesc() {
    if ( isInteger() ) {
      return ( getIsLongOrDouble() ? "Long" : "Integer" );
    }
    if ( isNumber() ) {
      return ( getIsLongOrDouble() ? "Double" : "Float" );
    }

    return ValueMeta.getTypeDesc( getType() );
  }

  @Override
  public Object decodeColumnValue( byte[] rawColValue ) throws KettleException {
    return Bytes.toString( rawColValue );
  }

  @Override
  public byte[] encodeColumnValue( Object columnValue, ValueMetaInterface colMeta ) throws KettleException {
    byte[] encoded = null;
    switch ( colMeta.getType() ){
      case TYPE_NUMBER:
        Double d = colMeta.getNumber( columnValue );
        encoded = Bytes.toBytes( d.floatValue() );
        break;
      case TYPE_INTEGER:
        Long l = colMeta.getInteger( columnValue );
        encoded = Bytes.toBytes( l );
        break;
      default:
        encoded = Bytes.toBytes( colMeta.getString( columnValue ) );
    }
    return encoded;
  }

  @Override
  public String getMappingName() {
    return mappingName;
  }

  @Override
  public void setMappingName( String mappingName ) {
    this.mappingName = mappingName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  @Override
  public boolean getIsLongOrDouble() {
    return isLongOrDouble;
  }

  @Override
  public void setIsLongOrDouble( boolean ld ) {
    this.isLongOrDouble = ld;
  }

  @Override
  public void getXml( StringBuilder retval ) {
    String format = getConversionMask();
    retval.append( "\n        " ).append( XMLHandler.openTag( "field" ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "table_name", getTableName() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "mapping_name", getMappingName() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "alias", getAlias() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "family", getColumnFamily() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "column", getColumnName() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "key", isKey() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "type", ValueMetaBase.getTypeDesc( getType() ) ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "format", format ) );
    retval.append( "\n        " ).append( XMLHandler.closeTag( "field" ) );
  }

  @Override
  public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int count ) throws KettleException {
    //noop in AEL
  }
}
