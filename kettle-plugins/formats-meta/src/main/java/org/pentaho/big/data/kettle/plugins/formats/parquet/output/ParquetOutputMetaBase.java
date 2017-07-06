package org.pentaho.big.data.kettle.plugins.formats.parquet.output;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.big.data.kettle.plugins.formats.FormatInputField;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

public abstract class ParquetOutputMetaBase extends BaseStepMeta implements StepMetaInterface {

  private String writerVersion;
  private int rowGroupSize;
  private int pageSize;
  private boolean enableDictionary;
  private int dictionaryPageSize;

  private String filename;

  private List<FormatInputField> outputFields = new ArrayList<FormatInputField>();

  @Override
  public void setDefault() {
    // TODO Auto-generated method stub
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public List<FormatInputField> getOutputFields() {
    return outputFields;
  }

  public void setOutputFields( List<FormatInputField> outputFields ) {
    this.outputFields = outputFields;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode, metaStore );
  }

  private void readData( Node stepnode, IMetaStore metastore ) throws KettleXMLException {
    try {
      filename = XMLHandler.getTagValue( stepnode, "filename" );
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );
      List<FormatInputField> parquetOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        FormatInputField outputField = new FormatInputField();
        outputField.setName( XMLHandler.getTagValue( fnode, "name" ) );
        outputField.setType( XMLHandler.getTagValue( fnode, "type" ) );
        parquetOutputFields.add( outputField );
      }
      this.outputFields = parquetOutputFields;
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 800 );

    retval.append( "    " + XMLHandler.addTagValue( "filename", filename ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.size(); i++ ) {
      FormatInputField field = outputFields.get( i );

      if ( field.getName() != null && field.getName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getTypeDesc() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      filename = rep.getStepAttributeString( id_step, "filename" );

      int nrfields = rep.countNrStepAttributes( id_step, "field" );

      List<FormatInputField> parquetOutputFields = new ArrayList<>();
      for ( int i = 0; i < nrfields; i++ ) {
        FormatInputField outputField = new FormatInputField();

        outputField.setName( rep.getStepAttributeString( id_step, i, "name" ) );
        outputField.setType( rep.getStepAttributeString( id_step, i, "type" ) );

        parquetOutputFields.add( outputField );
      }
      this.outputFields = parquetOutputFields;
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "filename", filename );
      for ( int i = 0; i < outputFields.size(); i++ ) {
        FormatInputField field = outputFields.get( i );

        rep.saveStepAttribute( id_transformation, id_step, i, "name", field.getName() );
        rep.saveStepAttribute( id_transformation, id_step, i, "type", field.getTypeDesc() );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

}
