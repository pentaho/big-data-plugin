/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

/**
 * Parquet input meta step without Hadoop-dependent classes. Required for read meta in the spark native code.
 * 
 * @author <alexander_buloichik@epam.com>
 */
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
