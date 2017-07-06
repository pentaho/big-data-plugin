package org.pentaho.big.data.kettle.plugins.formats.parquet.input;

import java.util.List;

import org.pentaho.big.data.kettle.plugins.formats.FormatInputField;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputFile;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.steps.file.BaseFileInputAdditionalField;
import org.pentaho.di.trans.steps.file.BaseFileInputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

public abstract class ParquetInputMetaBase extends
    BaseFileInputMeta<BaseFileInputAdditionalField, FormatInputFile, FormatInputField> {

  protected String dir;

  public ParquetInputMetaBase() {
    additionalOutputFields = new BaseFileInputAdditionalField();
    inputFiles = new FormatInputFile();
    inputFields = new FormatInputField[0];
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 1500 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "dir", dir ) );

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    dir = XMLHandler.getTagValue( stepnode, "dir" );
  }

  /**
   * TODO: remove from base
   */
  @Override
  public String getEncoding() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setDefault() {
    // TODO Auto-generated method stub

  }
  
  public String getDir() {
    return dir;
  }
}
