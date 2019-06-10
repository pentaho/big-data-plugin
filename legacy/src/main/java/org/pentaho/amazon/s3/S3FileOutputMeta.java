/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon.s3;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step( id = "S3FileOutputPlugin", image = "S3O.svg", name = "S3FileOutput.Name",
    description = "S3FileOutput.Description",
    documentationUrl = "Products/157_S3_File_Output",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Output",
    i18nPackageName = "org.pentaho.amazon.s3" )
@InjectionSupported( localizationPrefix = "S3FileOutput.Injection.", groups = { "OUTPUT_FIELDS" } )
public class S3FileOutputMeta extends TextFileOutputMeta {

  private static final String ACCESS_KEY_TAG = "access_key";
  private static final String SECRET_KEY_TAG = "secret_key";
  private static final String FILE_TAG = "file";
  private static final String NAME_TAG = "name";

  private static final Pattern OLD_STYLE_FILENAME = Pattern.compile( "^[s|S]3:\\/\\/([0-9a-zA-Z]{20}):(.+)@(.+)$" );


  private String accessKey = null;

  private String secretKey = null;

  public String getAccessKey() {
    return accessKey;
  }
  public void setAccessKey( String accessKey ) {
    this.accessKey = accessKey;
  }
  public String getSecretKey() {
    return secretKey;
  }
  public void setSecretKey( String secretKey ) {
    this.secretKey = secretKey;
  }

  @Override
  public void setDefault() {
    // call the base classes method
    super.setDefault();

    // now set the default for the
    // filename to an empty string
    setFileName( "s3n://s3n" );
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 1000 );
    retval.append( super.getXML() );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( ACCESS_KEY_TAG, Encr.encryptPasswordIfNotUsingVariables( accessKey ) ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( SECRET_KEY_TAG, Encr.encryptPasswordIfNotUsingVariables( secretKey ) ) );
    return retval.toString();
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      super.saveRep( rep, metaStore, id_transformation, id_step );
      rep.saveStepAttribute( id_transformation, id_step, ACCESS_KEY_TAG, Encr
        .encryptPasswordIfNotUsingVariables( accessKey ) );
      rep.saveStepAttribute( id_transformation, id_step, SECRET_KEY_TAG, Encr
        .encryptPasswordIfNotUsingVariables( secretKey ) );
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      super.readRep( rep, metaStore, id_step, databases );
      setAccessKey( Encr.decryptPasswordOptionallyEncrypted( rep.getStepAttributeString( id_step, ACCESS_KEY_TAG ) ) );
      setSecretKey( Encr.decryptPasswordOptionallyEncrypted( rep.getStepAttributeString( id_step, SECRET_KEY_TAG ) ) );
      String filename = rep.getStepAttributeString( id_step, "file_name" );
      processFilename( filename );
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  @Override
  public void readData( Node stepnode ) throws KettleXMLException {
    try {
      super.readData( stepnode );
      accessKey = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( stepnode, ACCESS_KEY_TAG ) );
      secretKey = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( stepnode, SECRET_KEY_TAG ) );
      String filename = XMLHandler.getTagValue( stepnode, FILE_TAG, NAME_TAG );
      processFilename( filename );
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans ) {
    return new S3FileOutput( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  /**
   * New filenames obey the rule s3://<any_string>/<s3_bucket_name>/<path>. However, we maintain old filenames
   * s3://<access_key>:<secret_key>@s3/<s3_bucket_name>/<path>
   *
   * @param filename
   * @return
   */
  protected void processFilename( String filename ) throws Exception {
    if ( Const.isEmpty( filename ) ) {
      filename = "s3n://s3n/";
      setFileName( filename );
      return;
    }
    // it it's an old-style filename - use and then remove keys from the filename
    Matcher matcher = OLD_STYLE_FILENAME.matcher( filename );
    if ( matcher.matches() ) {
      // old style filename is URL encoded
      accessKey = decodeAccessKey( matcher.group( 1 ) );
      secretKey = decodeAccessKey( matcher.group( 2 ) );
      setFileName( "s3://" + matcher.group( 3 ) );
    } else {
      setFileName( filename );
    }
  }

  protected String decodeAccessKey( String key ) {
    if ( Const.isEmpty( key ) ) {
      return key;
    }
    return key.replaceAll( "%2B", "\\+" ).replaceAll( "%2F", "/" );
  }

}
