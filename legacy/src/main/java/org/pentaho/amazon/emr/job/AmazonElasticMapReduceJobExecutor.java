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

package org.pentaho.amazon.emr.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

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
import org.w3c.dom.Node;

@JobEntry( id = "EMRJobExecutorPlugin", image = "EMR.svg", name = "EMRJobExecutorPlugin.Name",
  description = "EMRJobExecutorPlugin.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
  documentationUrl = "Products/Amazon_EMR_Job_Executor",
  i18nPackageName = "org.pentaho.amazon.emr.job" )
public class AmazonElasticMapReduceJobExecutor extends AbstractAmazonJobExecutor {

  private static Class<?> PKG = AmazonElasticMapReduceJobExecutor.class;
  private static final String STEP_EMR = "emr";
  private URL localFileUrl;

  protected String jarUrl = "";

  public String getJarUrl() {
    return jarUrl;
  }

  public void setJarUrl( String jarUrl ) {
    this.jarUrl = jarUrl;
  }

  public AmazonElasticMapReduceJobExecutor() {
  }

  public String getMainClass( URL localJarUrl ) throws Exception {
    //todo: here we should select named cluster according to step or url
//    HadoopShim shim = null;
//    HadoopConfigurationBootstrap.getHadoopConfigurationProvider().getActiveConfiguration().getHadoopShim();

    final Class<?> mainClass = getMainClassFromManifest( localJarUrl, null /*shim.getClass().getClassLoader()*/ );
    if ( mainClass != null ) {
      return mainClass.getName();
    } else {
      List<Class<?>> classesWithMains =
          getClassesInJarWithMain( localJarUrl.toExternalForm(), null /*shim.getClass().getClassLoader()*/ );
      if ( !classesWithMains.isEmpty() ) {
        return classesWithMains.get( 0 ).getName();
      }
    }
    throw new RuntimeException( "Could not find main class in: " + localJarUrl.toExternalForm() );
  }

  public boolean isAlive() {
    return alive;
  }

  @Override
  public File createStagingFile() throws IOException, KettleException {
    // pull down .jar file from VSF
    FileObject jarFile = KettleVFS.getFileObject( buildFilename( jarUrl ) );
    File tmpFile = File.createTempFile( "customEMR", "jar" );
    tmpFile.deleteOnExit();
    FileOutputStream tmpFileOut = new FileOutputStream( tmpFile );
    IOUtils.copy( jarFile.getContent().getInputStream(), tmpFileOut );
    localFileUrl = tmpFile.toURI().toURL();
    setS3BucketKey( jarFile );
    return tmpFile;
  }

  @Override
  public String getStepBootstrapActions() {
    return null;
  }

  @Override
  public String getMainClass() throws Exception {
    return getMainClass( localFileUrl );
  }

  public String getStepType() {
    return STEP_EMR;
  }

  @Override
  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers,
                       Repository rep, IMetaStore metaStore )
    throws KettleXMLException {
    super.loadXML( entrynode, databases, slaveServers );
    hadoopJobName = XMLHandler.getTagValue( entrynode, "hadoop_job_name" );
    hadoopJobFlowId = XMLHandler.getTagValue( entrynode, "hadoop_job_flow_id" );
    jarUrl = XMLHandler.getTagValue( entrynode, "jar_url" );
    accessKey = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( entrynode, "access_key" ) );
    secretKey = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( entrynode, "secret_key" ) );
    stagingDir = XMLHandler.getTagValue( entrynode, "staging_dir" );
    region = XMLHandler.getTagValue( entrynode, "region" );
    ec2Role = XMLHandler.getTagValue( entrynode, "ec2_role" );
    emrRole = XMLHandler.getTagValue( entrynode, "emr_role" );
    masterInstanceType = XMLHandler.getTagValue( entrynode, "master_instance_type" );
    slaveInstanceType = XMLHandler.getTagValue( entrynode, "slave_instance_type" );
    numInstances = XMLHandler.getTagValue( entrynode, "num_instances" );
    emrRelease = XMLHandler.getTagValue( entrynode, "emr_release" );
    cmdLineArgs = XMLHandler.getTagValue( entrynode, "command_line_args" );
    alive = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "alive" ) );
    runOnNewCluster = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "runOnNewCluster" ) );
    blocking = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "blocking" ) );
    loggingInterval = XMLHandler.getTagValue( entrynode, "logging_interval" );
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 1024 );
    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "hadoop_job_name", hadoopJobName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "hadoop_job_flow_id", hadoopJobFlowId ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "jar_url", jarUrl ) );
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
    retval.append( "      " ).append( XMLHandler.addTagValue( "staging_dir", stagingDir ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "command_line_args", cmdLineArgs ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "alive", alive ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "runOnNewCluster", runOnNewCluster ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "blocking", blocking ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "logging_interval", loggingInterval ) );

    return retval.toString();
  }

  @Override
  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
                       List<SlaveServer> slaveServers ) throws KettleException {
    if ( rep != null ) {
      super.loadRep( rep, metaStore, id_jobentry, databases, slaveServers );

      setHadoopJobName( rep.getJobEntryAttributeString( id_jobentry, "hadoop_job_name" ) );
      setHadoopJobFlowId( rep.getJobEntryAttributeString( id_jobentry, "hadoop_job_flow_id" ) );
      setJarUrl( rep.getJobEntryAttributeString( id_jobentry, "jar_url" ) );
      setAccessKey( Encr
        .decryptPasswordOptionallyEncrypted( rep.getJobEntryAttributeString( id_jobentry, "access_key" ) ) );
      setSecretKey( Encr
        .decryptPasswordOptionallyEncrypted( rep.getJobEntryAttributeString( id_jobentry, "secret_key" ) ) );
      setStagingDir( rep.getJobEntryAttributeString( id_jobentry, "staging_dir" ) );
      setRegion( rep.getJobEntryAttributeString( id_jobentry, "region" ) );
      setEc2Role( rep.getJobEntryAttributeString( id_jobentry, "ec2_role" ) );
      setEmrRole( rep.getJobEntryAttributeString( id_jobentry, "emr_role" ) );
      setMasterInstanceType( rep.getJobEntryAttributeString( id_jobentry, "master_instance_type" ) );
      setSlaveInstanceType( rep.getJobEntryAttributeString( id_jobentry, "slave_instance_type" ) );
      setEmrRelease( rep.getJobEntryAttributeString( id_jobentry, "emr_release" ) );
      setNumInstances( rep.getJobEntryAttributeString( id_jobentry, "num_instances" ) );
      setCmdLineArgs( rep.getJobEntryAttributeString( id_jobentry, "command_line_args" ) );
      setAlive( rep.getJobEntryAttributeBoolean( id_jobentry, "alive" ) );
      setRunOnNewCluster( rep.getJobEntryAttributeBoolean( id_jobentry, "runOnNewCluster" ) );
      setBlocking( rep.getJobEntryAttributeBoolean( id_jobentry, "blocking" ) );
      setLoggingInterval( rep.getJobEntryAttributeString( id_jobentry, "logging_interval" ) );

    } else {
      throw new KettleException( BaseMessages.getString( PKG,
        "AmazonElasticMapReduceJobExecutor.LoadFromRepository.Error" ) );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
    if ( rep != null ) {
      super.saveRep( rep, metaStore, id_job );

      rep.saveJobEntryAttribute( id_job, getObjectId(), "hadoop_job_name", hadoopJobName );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "hadoop_job_flow_id", hadoopJobFlowId );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "jar_url", jarUrl );
      rep.saveJobEntryAttribute( id_job, getObjectId(),
        "secret_key", Encr.encryptPasswordIfNotUsingVariables( secretKey ) );
      rep.saveJobEntryAttribute( id_job, getObjectId(),
        "access_key", Encr.encryptPasswordIfNotUsingVariables( accessKey ) );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "staging_dir", stagingDir );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "region", region );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "ec2_role", ec2Role );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "emr_role", emrRole );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "master_instance_type", masterInstanceType );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "slave_instance_type", slaveInstanceType );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "emr_release", emrRelease );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "num_instances", numInstances );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "command_line_args", cmdLineArgs );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "alive", alive );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "runOnNewCluster", runOnNewCluster );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "blocking", blocking );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "logging_interval", loggingInterval );

    } else {
      throw new KettleException(
        BaseMessages.getString( PKG, "AmazonElasticMapReduceJobExecutor.SaveToRepository.Error" ) );
    }
  }

  public String buildFilename( String filename ) {
    filename = environmentSubstitute( filename );
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

  @Override
  public String getDialogClassName() {
    String className = getClass().getCanonicalName();
    className = className.replaceFirst( "\\.job\\.", ".ui." );
    className += "Dialog";
    return className;
  }

  private Class<?> getMainClassFromManifest( URL jarUrl, ClassLoader parentClassLoader )
          throws IOException, ClassNotFoundException {
    JarFile jarFile = getJarFile( jarUrl, parentClassLoader );
    try {
      Manifest manifest = jarFile.getManifest();
      String className = manifest == null ? null : manifest.getMainAttributes().getValue( "Main-Class" );
      return loadClassByName( className, jarUrl, parentClassLoader );
    } finally {
      jarFile.close();
    }
  }

  private JarFile getJarFile( final URL jarUrl, final ClassLoader parentClassLoader ) throws IOException {
    if ( jarUrl == null || parentClassLoader == null ) {
      throw new NullPointerException();
    }
    JarFile jarFile;
    try {
      jarFile = new JarFile( new File( jarUrl.toURI() ) );
    } catch ( URISyntaxException ex ) {
      throw new IOException( "Error locating jar: " + jarUrl );
    } catch ( IOException ex ) {
      throw new IOException( "Error opening job jar: " + jarUrl, ex );
    }
    return jarFile;
  }

  private Class<?> loadClassByName( final String className, final URL jarUrl, final ClassLoader parentClassLoader )
          throws ClassNotFoundException {
    if ( className != null ) {
      URLClassLoader cl = new URLClassLoader( new URL[] { jarUrl }, parentClassLoader );
      Class<?> clazz = cl.loadClass( className.replaceAll( "/", "." ) );
      try {
        cl.close();
      } catch ( IOException e ) {
      }
      return clazz;
    } else {
      return null;
    }
  }

  public List<Class<?>> getClassesInJarWithMain( String jarUrl, ClassLoader parentClassloader )
          throws MalformedURLException {
    ArrayList<Class<?>> mainClasses = new ArrayList<Class<?>>();
    List<Class<?>> allClasses = getClassesInJar( jarUrl, parentClassloader );
    for ( Class<?> clazz : allClasses ) {
      try {
        Method mainMethod = clazz.getMethod( "main", new Class[] { String[].class } );
        if ( Modifier.isStatic( mainMethod.getModifiers() ) ) {
          mainClasses.add( clazz );
        }
      } catch ( Throwable ignored ) {
        // Ignore classes without main() methods
      }
    }
    return mainClasses;
  }

  public List<Class<?>> getClassesInJar( String jarUrl, ClassLoader parentClassloader )
          throws MalformedURLException {
    ArrayList<Class<?>> classes = new ArrayList<Class<?>>();
    URL url = new URL( jarUrl );
    URL[] urls = new URL[] { url };
    try ( URLClassLoader loader = new URLClassLoader( urls, getClass().getClassLoader() );
          JarInputStream jarFile = new JarInputStream( new FileInputStream( new File( url.toURI() ) ) ) ) {
      while ( true ) {
        JarEntry jarEntry = jarFile.getNextJarEntry();
        if ( jarEntry == null ) {
          break;
        }
        if ( jarEntry.getName().endsWith( ".class" ) ) {
          String className = jarEntry.getName().substring( 0, jarEntry.getName().indexOf( ".class" ) ).replaceAll( "/", "\\." );
          classes.add( loader.loadClass( className ) );
        }
      }
    } catch ( IOException e ) {
    } catch ( ClassNotFoundException e ) {
    } catch ( URISyntaxException e ) {
    }
    return classes;
  }

}
