package org.pentaho.big.data.kettle.plugins.hbase;

import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;

public class AELHBaseMappingImpl implements Mapping {

  private String tableName;
  private String mappingName;
  private String keyName;
  private KeyType keyType;
  private String keyTypeAsString;
  private int numMappedColumns;
  private Map<String, HBaseValueMetaInterface> mappedColumns;

  public AELHBaseMappingImpl() {
  }

//  public AELHBaseMappingImpl( String tableName, String mappingName, String keyName, String keyTypeAsString, Map<String, HBaseValueMetaInterface> mappedColumns ){
//    this.tableName = tableName;
//    this.mappingName = mappingName;
//    this.keyName = keyName;
//    this.keyTypeAsString = keyTypeAsString;
//    //this.keyType = KeyType.valueOf( keyTypeAsString );
//    this.numMappedColumns = mappedColumns.size();
//    this.mappedColumns = mappedColumns;
//  }
//
//  public AELHBaseMappingImpl( String tableName, Mapping rawMapping, Map<String, HBaseValueMetaInterface> mappedColumns ) {
//    this.tableName = tableName;
//    this.keyName = rawMapping.getKeyName();
//    this.keyTypeAsString = rawMapping.getKeyType().toString();
//    this.numMappedColumns = mappedColumns.size();
//    this.mappedColumns = mappedColumns;
//  }

  @Override
  public String addMappedColumn( HBaseValueMetaInterface hBaseValueMetaInterface, boolean b ) throws Exception {
    if ( mappedColumns == null ) {
      mappedColumns = new HashMap<>();
    }

    mappedColumns.put( hBaseValueMetaInterface.getAlias(), hBaseValueMetaInterface );
    this.numMappedColumns++;

    return hBaseValueMetaInterface.getAlias();
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
  public String getMappingName() {
    return mappingName;
  }

  @Override
  public void setMappingName( String mappingName ) {
    this.mappingName = mappingName;
  }

  @Override
  public String getKeyName() {
    return keyName;
  }

  @Override
  public void setKeyName( String keyName ) {
    this.keyName = keyName;
  }

  @Override
  public KeyType getKeyType() {
    return keyType;
  }

  @Override
  public void setKeyType( KeyType keyType ) {
    this.keyType = keyType;
  }

  @Override
  public Map<String, HBaseValueMetaInterface> getMappedColumns() {
    return mappedColumns;
  }

  @Override
  public void setMappedColumns( Map<String, HBaseValueMetaInterface> mappedColumns ) {
    this.mappedColumns = mappedColumns;
  }

  @Override
  public void setKeyTypeAsString( String s ) throws Exception {
    this.keyTypeAsString = s;
  }

  @Override
  public boolean isTupleMapping() {
    return false;
  }

  @Override
  public void setTupleMapping( boolean b ) {

  }

  @Override
  public String getTupleFamilies() {
    return null;
  }

  @Override
  public String[] getTupleFamiliesSplit() {
    return new String[0];
  }

  @Override
  public void setTupleFamilies( String s ) {

  }

  @Override
  public int numMappedColumns() {
    return this.numMappedColumns;
  }

  @Override
  public void saveRep( Repository repository, ObjectId objectId, ObjectId objectId1 ) throws KettleException {
    //noop on AEL
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer();

    if ( Const.isEmpty( getKeyName() ) ) {
      return ""; // nothing defined
    }

    retval.append( "\n     " ).append( XMLHandler.openTag( "mapping" ) );
    retval.append( "\n      " ).append(
        XMLHandler.addTagValue( "mapping_name", getMappingName() ) );
    retval.append( "\n      " ).append(
        XMLHandler.addTagValue( "table_name", getTableName() ) );

    retval.append( "\n      " ).append( XMLHandler.addTagValue( "key", getKeyName() ) );
    retval.append( "\n      " ).append(
        XMLHandler.addTagValue( "key_type", getKeyType().toString() ) );

    if ( mappedColumns.size() > 0 ) {
      retval.append( "\n        " ).append( XMLHandler.openTag( "mapped_columns" ) );

      for ( String alias : mappedColumns.keySet() ) {
        HBaseValueMetaInterface vm = mappedColumns.get( alias );

        retval.append( "\n        " ).append( XMLHandler.openTag( "mapped_column" ) );

        retval.append( "\n          " ).append(
            XMLHandler.addTagValue( "alias", alias ) );
        retval.append( "\n          " ).append(
            XMLHandler.addTagValue( "column_family", vm.getColumnFamily() ) );
        retval.append( "\n          " ).append(
            XMLHandler.addTagValue( "column_name", vm.getColumnName() ) );
        retval.append( "\n          " ).append(
            XMLHandler.addTagValue( "type",
                vm.getHBaseTypeDesc() ) );
      }

      retval.append( "\n        " )
          .append( XMLHandler.closeTag( "mapped_column" ) );
    }
    retval.append( "\n        " ).append( XMLHandler.closeTag( "mapped_columns" ) );

    retval.append( "\n     " ).append( XMLHandler.closeTag( "mapping" ) );

    return retval.toString();
  }

  @Override
  public boolean loadXML( Node node ) throws KettleXMLException {
    node = XMLHandler.getSubNode( node, "mapping" );

    if ( node == null
        || Const.isEmpty( XMLHandler.getTagValue( node, "key" ) ) ) {
      return false; // no mapping info in XML
    }

    setMappingName( XMLHandler.getTagValue( node, "mapping_name" ) );
    setTableName( XMLHandler.getTagValue( node, "table_name" ) );

    String keyName = XMLHandler.getTagValue( node, "key" );
    if ( keyName.indexOf( ',' ) > 0 ) {
      setTupleMapping( true );
      setKeyName( keyName.substring( 0, keyName.indexOf( ',' ) ) );
      if ( keyName.indexOf( ',' ) != keyName.length() - 1 ) {
        // specific families have been supplied
        String familiesList = keyName.substring( keyName.indexOf( ',' ) + 1,
            keyName.length() );
        if ( !Const.isEmpty( familiesList.trim() ) ) {
          setTupleFamilies( familiesList );
        }
      }
    } else {
      setKeyName( keyName );
    }

    String keyTypeS = XMLHandler.getTagValue( node, "key_type" );
    for ( KeyType k : KeyType.values() ) {
      if ( k.toString().equalsIgnoreCase( keyTypeS ) ) {
        setKeyType( k );
        break;
      }
    }

    Node fields = XMLHandler.getSubNode( node, "mapped_columns" );
    if ( fields != null && XMLHandler.countNodes( fields, "mapped_column" ) > 0 ) {
      int nrfields = XMLHandler.countNodes( fields, "mapped_column" );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fieldNode = XMLHandler.getSubNodeByNr( fields, "mapped_column", i );
        String alias = XMLHandler.getTagValue( fieldNode, "alias" );
        String colFam = XMLHandler.getTagValue( fieldNode, "column_family" );
        if ( colFam == null ) {
          colFam = "";
        }
        String colName = XMLHandler.getTagValue( fieldNode, "column_name" );
        if ( colName == null ) {
          colName = "";
        }
        String type = XMLHandler.getTagValue( fieldNode, "type" );

        AELHBaseValueMetaImpl vm = new AELHBaseValueMetaImpl( false, alias, colName, colFam, getMappingName(), getTableName(), type );
        vm.setHBaseTypeFromString( type );

//        String indexedV = XMLHandler.getTagValue( fieldNode, "indexed_vals" );
//        if ( !Const.isEmpty( indexedV ) ) {
//          Object[] nomVals = AELHBaseValueMetaImpl.stringIndexListToObjects( indexedV );
//          hbvm.setIndex( nomVals );
//          hbvm.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
//        }

        try {
          addMappedColumn( vm, isTupleMapping() );
        } catch ( Exception ex ) {
          throw new KettleXMLException( ex );
        }
      }
    }

    return true;
  }

  @Override
  public boolean readRep( Repository repository, ObjectId objectId ) throws KettleException {
    return false;
  }

  @Override
  public String getFriendlyName() {
    return null;
  }

  @Override
  public Object decodeKeyValue( byte[] bytes ) throws KettleException {
    return null;
  }
}
