/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon.hive.job;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.amazon.AbstractAmazonJobExecutor;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.s3.vfs.S3FileProvider;
import org.w3c.dom.Node;

/**
 * AmazonHiveJobExecutor A job entry plug-in class to submits a Hive job into the AWS Elastic MapReduce service from
 * Pentaho Data Integration (Kettle).
 */
@JobEntry( id = "HiveJobExecutorPlugin", image = "AWS-HIVE.svg", name = "HiveJobExecutorPlugin.Name",
  description = "HiveJobExecutorPlugin.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
  documentationUrl = "Products/Amazon_Hive_Job_Executor",
  i18nPackageName = "org.pentaho.amazon.hive.job" )
public class AmazonHiveJobExecutor extends AbstractAmazonJobExecutor {

  private static Class<?> PKG = AmazonHiveJobExecutor.class;
  private static final String STEP_HIVE = "hive";

  protected String qUrl = "";
  protected String bootstrapActions = "";

  public AmazonHiveJobExecutor() {
  }

  public String getQUrl() {
    return qUrl;
  }

  public void setQUrl( String qUrl ) {
    this.qUrl = qUrl;
  }

  public String getBootstrapActions() {
    return bootstrapActions;
  }

  public void setBootstrapActions( String bootstrapActions ) {
    this.bootstrapActions = bootstrapActions;
  }

  public boolean isAlive() {
    return alive;
  }

  @Override
  public File createStagingFile() throws IOException, KettleException {
    // pull down .q file from VSF
    FileObject qFile = KettleVFS.getFileObject( buildFilename( qUrl ) );
    File tmpFile = File.createTempFile( "customEMR", "q" );
    tmpFile.deleteOnExit();
    FileOutputStream tmpFileOut = new FileOutputStream( tmpFile );
    IOUtils.copy( qFile.getContent().getInputStream(), tmpFileOut );
    //localFileUrl = tmpFile.toURI().toURL();
    setS3BucketKey( qFile );
    return tmpFile;
  }

  @Override
  public String getStepBootstrapActions() {
    return bootstrapActions;
  }

  @Override
  public String getMainClass() throws Exception {
    return null;
  }

  public String getStepType() {
    return STEP_HIVE;
  }

  /**
   * Load attributes
   */
  @Override
  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers,
                       Repository rep, IMetaStore metaStore )
    throws KettleXMLException {
    super.loadXML( entrynode, databases, slaveServers );
    hadoopJobName = XMLHandler.getTagValue( entrynode, "hadoop_job_name" );
    hadoopJobFlowId = XMLHandler.getTagValue( entrynode, "hadoop_job_flow_id" );
    qUrl = XMLHandler.getTagValue( entrynode, "q_url" );
    accessKey =
      Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( entrynode, "access_key" ) );
    secretKey =
      Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( entrynode, "secret_key" ) );
    region = XMLHandler.getTagValue( entrynode, "region" );
    ec2Role = XMLHandler.getTagValue( entrynode, "ec2_role" );
    emrRole = XMLHandler.getTagValue( entrynode, "emr_role" );
    masterInstanceType = XMLHandler.getTagValue( entrynode, "master_instance_type" );
    slaveInstanceType = XMLHandler.getTagValue( entrynode, "slave_instance_type" );
    numInstances = XMLHandler.getTagValue( entrynode, "num_instances" );
    emrRelease = XMLHandler.getTagValue( entrynode, "emr_release" );
    //selectedInstanceType = XMLHandler.getTagValue( entrynode, "selected_instance_type" );
    bootstrapActions = XMLHandler.getTagValue( entrynode, "bootstrap_actions" );
    stagingDir = XMLHandler.getTagValue( entrynode, "staging_dir" );
    cmdLineArgs = XMLHandler.getTagValue( entrynode, "command_line_args" );
    alive = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "alive" ) );
    runOnNewCluster = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "runOnNewCluster" ) );
    blocking = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "blocking" ) );
    loggingInterval = XMLHandler.getTagValue( entrynode, "logging_interval" );
  }

  /**
   * Get attributes
   */
  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 1024 );
    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "hadoop_job_name", hadoopJobName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "hadoop_job_flow_id", hadoopJobFlowId ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "q_url", qUrl ) );
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "access_key", Encr.encryptPasswordIfNotUsingVariables( accessKey ) ) );
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "secret_key", Encr.encryptPasswordIfNotUsingVariables( secretKey ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "region", region ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "ec2_role", ec2Role ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "emr_role", emrRole ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "master_instance_type", masterInstanceType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "slave_instance_type", slaveInstanceType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "emr_release", emrRelease ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "num_instances", numInstances ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "bootstrap_actions", bootstrapActions ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "staging_dir", stagingDir ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "command_line_args", cmdLineArgs ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "alive", alive ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "runOnNewCluster", runOnNewCluster ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "blocking", blocking ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "logging_interval", loggingInterval ) );

    return retval.toString();
  }

  /**
   * Load attributes from a repository
   */
  @Override
  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
                       List<SlaveServer> slaveServers ) throws KettleException {
    if ( rep != null ) {
      super.loadRep( rep, metaStore, id_jobentry, databases, slaveServers );

      setHadoopJobName( rep.getJobEntryAttributeString( id_jobentry, "hadoop_job_name" ) );
      setHadoopJobFlowId( rep.getJobEntryAttributeString( id_jobentry, "hadoop_job_flow_id" ) );
      setQUrl( rep.getJobEntryAttributeString( id_jobentry, "q_url" ) );
      setAccessKey( Encr
        .decryptPasswordOptionallyEncrypted(
          rep.getJobEntryAttributeString( id_jobentry, "access_key" ) ) );
      setSecretKey( Encr
        .decryptPasswordOptionallyEncrypted(
          rep.getJobEntryAttributeString( id_jobentry, "secret_key" ) ) );
      setRegion( rep.getJobEntryAttributeString( id_jobentry, "region" ) );
      setEc2Role( rep.getJobEntryAttributeString( id_jobentry, "ec2_role" ) );
      setEmrRole( rep.getJobEntryAttributeString( id_jobentry, "emr_role" ) );
      setMasterInstanceType( rep.getJobEntryAttributeString( id_jobentry, "master_instance_type" ) );
      setSlaveInstanceType( rep.getJobEntryAttributeString( id_jobentry, "slave_instance_type" ) );
      setEmrRelease( rep.getJobEntryAttributeString( id_jobentry, "emr_release" ) );
      setNumInstances( rep.getJobEntryAttributeString( id_jobentry, "num_instances" ) );
      setBootstrapActions( rep.getJobEntryAttributeString( id_jobentry, "bootstrap_actions" ) );
      setStagingDir( rep.getJobEntryAttributeString( id_jobentry, "staging_dir" ) );
      setCmdLineArgs( rep.getJobEntryAttributeString( id_jobentry, "command_line_args" ) );
      setAlive( rep.getJobEntryAttributeBoolean( id_jobentry, "alive" ) );
      setRunOnNewCluster( rep.getJobEntryAttributeBoolean( id_jobentry, "runOnNewCluster" ) );
      setBlocking( rep.getJobEntryAttributeBoolean( id_jobentry, "blocking" ) );
      setLoggingInterval( rep.getJobEntryAttributeString( id_jobentry, "logging_interval" ) );

    } else {
      throw new KettleException( BaseMessages.getString( PKG, "AmazonHiveJobExecutor.LoadFromRepository.Error" ) );
    }
  }

  /**
   * Save attributes to a repository
   */
  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
    if ( rep != null ) {
      super.saveRep( rep, metaStore, id_job );

      rep.saveJobEntryAttribute( id_job, getObjectId(), "hadoop_job_name", hadoopJobName );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "hadoop_job_flow_id", hadoopJobFlowId );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "q_url", qUrl );
      rep.saveJobEntryAttribute( id_job, getObjectId(),
        "secret_key", Encr.encryptPasswordIfNotUsingVariables( secretKey ) );
      rep.saveJobEntryAttribute( id_job, getObjectId(),
        "access_key", Encr.encryptPasswordIfNotUsingVariables( accessKey ) );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "region", region );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "ec2_role", ec2Role );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "emr_role", emrRole );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "master_instance_type", masterInstanceType );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "slave_instance_type", slaveInstanceType );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "emr_release", emrRelease );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "num_instances", numInstances );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "bootstrap_actions", bootstrapActions );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "staging_dir", stagingDir );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "command_line_args", cmdLineArgs );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "alive", alive );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "blocking", blocking );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "runOnNewCluster", runOnNewCluster );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "logging_interval", loggingInterval );
    } else {
      throw new KettleException( BaseMessages.getString( PKG, "AmazonHiveJobExecutor.SaveToRepository.Error" ) );
    }
  }

  /**
   * Build S3 URL. Replace "/" and "\" with ASCII equivalents within the access/secret keys, otherwise VFS will have
   * trouble in parsing the filename.
   *
   * @param filename - S3 URL of a file with access/secret keys in it
   * @return S3 URL with "/" and "\" with ASCII equivalents within the access/secret keys
   */
  public String buildFilename( String filename ) {
    filename = environmentSubstitute( filename );
    if ( filename.startsWith( S3FileProvider.SCHEME ) ) {
      String authPart =
        filename
          .substring( S3FileProvider.SCHEME.length() + 3, filename.indexOf( "@s3" ) ).replaceAll( "\\+", "%2B" )
          .replaceAll( "/", "%2F" );
      filename = S3FileProvider.SCHEME + "://" + authPart + "@s3" + filename
        .substring( filename.indexOf( "@s3" ) + 3 );
    }
    return filename;
  }

  @Override
  public boolean evaluates() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  /**
   * Get the class name for the dialog box of this plug-in.
   */
  @Override
  public String getDialogClassName() {
    String className = getClass().getCanonicalName();
    className = className.replaceFirst( "\\.job\\.", ".ui." );
    className += "Dialog";
    return className;
  }
}
