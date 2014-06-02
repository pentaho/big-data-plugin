/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.job.entries.sqoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobEntryUtils;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.LoggingProxy;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryCreationHelper;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.common.CommonHadoopShim;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.w3c.dom.Document;

public class SqoopImportJobEntryTest {
  private SqoopImportJobEntry sqoopImportJobEntry;
  private LogChannelInterface mockLogChannelInterface;
  private Job mockParentJob;
  private JobMeta mockParentJobMeta;

  @BeforeClass
  public static void setupBeforeClass() throws KettleException {
    KettleClientEnvironment.init();
  }

  @Before
  public void setup() {
    mockLogChannelInterface = mock( LogChannelInterface.class );
    sqoopImportJobEntry = new SqoopImportJobEntry( mockLogChannelInterface );
    mockParentJob = mock( Job.class );
    mockParentJobMeta = mock( JobMeta.class );
    when( mockParentJob.getJobMeta() ).thenReturn( mockParentJobMeta );
    sqoopImportJobEntry.setParentJob( mockParentJob );
  }

  @Test
  public void buildSqoopConfig() {
    assertEquals( SqoopImportConfig.class, sqoopImportJobEntry.getJobConfig().getClass() );
  }

  @Test
  public void getToolName() {
    assertEquals( "import", sqoopImportJobEntry.getToolName() );
  }

  @Test
  public void saveLoadTest_xml() throws KettleException {
    SqoopImportConfig config = new SqoopImportConfig();
    String connectValue = "jdbc:mysql://localhost:3306/test";
    String myPassword = "my-password";

    config.setJobEntryName( "testing" );
    config.setBlockingExecution( "false" );
    config.setBlockingPollingInterval( "100" );
    config.setConnect( connectValue );
    config.setTargetDir( "/test-import-target" );
    config.setPassword( myPassword );
    config.setHbaseZookeeperQuorum( "test-zookeeper-host" );

    sqoopImportJobEntry.setJobConfig( config );

    JobEntryCopy jec = new JobEntryCopy( sqoopImportJobEntry );
    jec.setLocation( 0, 0 );
    String xml = jec.getXML();

    assertTrue( "Password not encrypted upon save to xml", !xml.contains( myPassword ) );

    Document d = XMLHandler.loadXMLString( xml );

    SqoopImportJobEntry je2 = new SqoopImportJobEntry( mockLogChannelInterface );
    je2.loadXML( d.getDocumentElement(), null, null, null );

    SqoopImportConfig config2 = je2.getJobConfig();
    assertEquals( config.getJobEntryName(), config2.getJobEntryName() );
    assertEquals( config.getBlockingExecution(), config2.getBlockingExecution() );
    assertEquals( config.getBlockingPollingInterval(), config2.getBlockingPollingInterval() );
    assertEquals( config.getConnect(), config2.getConnect() );
    assertEquals( config.getTargetDir(), config2.getTargetDir() );
    assertEquals( config.getPassword(), config2.getPassword() );
    assertEquals( config.getHbaseZookeeperQuorum(), config2.getHbaseZookeeperQuorum() );
  }

  @Test
  public void saveLoadTest_xml_advanced_options() throws KettleXMLException {
    SqoopImportConfig config = new SqoopImportConfig();
    String connectValue = "jdbc:mysql://localhost:3306/test";
    String myPassword = "my-password";

    config.setJobEntryName( "testing" );
    config.setBlockingExecution( "false" );
    config.setBlockingPollingInterval( "100" );
    config.setConnect( connectValue );
    config.setTargetDir( "/test-import-target" );
    config.setPassword( myPassword );
    config.setHbaseZookeeperQuorum( "test-zookeeper-host" );

    config.copyConnectionInfoToAdvanced();

    sqoopImportJobEntry.setJobConfig( config );

    JobEntryCopy jec = new JobEntryCopy( sqoopImportJobEntry );
    jec.setLocation( 0, 0 );
    String xml = jec.getXML();

    assertTrue( "Password not encrypted upon save to xml", !xml.contains( myPassword ) );

    Document d = XMLHandler.loadXMLString( xml );

    SqoopImportJobEntry je2 = new SqoopImportJobEntry( mockLogChannelInterface );
    je2.loadXML( d.getDocumentElement(), null, null, null );

    SqoopImportConfig config2 = je2.getJobConfig();
    assertEquals( config.getJobEntryName(), config2.getJobEntryName() );
    assertEquals( config.getBlockingExecution(), config2.getBlockingExecution() );
    assertEquals( config.getBlockingPollingInterval(), config2.getBlockingPollingInterval() );
    assertEquals( config.getConnect(), config2.getConnect() );
    assertEquals( config.getTargetDir(), config2.getTargetDir() );
    assertEquals( config.getPassword(), config2.getPassword() );
    assertEquals( config.getHbaseZookeeperQuorum(), config2.getHbaseZookeeperQuorum() );

    assertEquals( config.getConnectFromAdvanced(), config2.getConnectFromAdvanced() );
    assertEquals( config.getUsernameFromAdvanced(), config2.getUsernameFromAdvanced() );
    assertEquals( config.getPasswordFromAdvanced(), config2.getPasswordFromAdvanced() );
  }

  @Test
  public void saveLoadTest_rep() throws KettleException, IOException {
    SqoopImportConfig config = new SqoopImportConfig();
    String connectValue = "jdbc:mysql://localhost:3306/test";

    config.setJobEntryName( "testing" );
    config.setBlockingExecution( "false" );
    config.setBlockingPollingInterval( "100" );
    config.setConnect( connectValue );
    config.setTargetDir( "/test-import-target" );
    config.setHbaseZookeeperQuorum( "test-zookeeper-host" );

    sqoopImportJobEntry.setJobConfig( config );

    KettleEnvironment.init();
    String filename = File.createTempFile( getClass().getSimpleName() + "-import-dbtest", "" ).getAbsolutePath();

    try {
      DatabaseMeta databaseMeta = new DatabaseMeta( "H2Repo", "H2", "JDBC", null, filename, null, null, null );
      RepositoryMeta repositoryMeta =
          new KettleDatabaseRepositoryMeta( "KettleDatabaseRepository", "H2Repo", "H2 Repository", databaseMeta );
      KettleDatabaseRepository repository = new KettleDatabaseRepository();
      repository.init( repositoryMeta );
      repository.connectionDelegate.connect( true, true );
      KettleDatabaseRepositoryCreationHelper helper = new KettleDatabaseRepositoryCreationHelper( repository );
      helper.createRepositorySchema( null, false, new ArrayList<String>(), false );
      repository.disconnect();

      // Test connecting...
      //
      repository.connect( "admin", "admin" );
      assertTrue( repository.isConnected() );

      // A job entry must have an ID if we're going to save it to a repository
      sqoopImportJobEntry.setObjectId( new LongObjectId( 1 ) );
      ObjectId id_job = new LongObjectId( 1 );

      // Save the original job entry into the repository
      sqoopImportJobEntry.saveRep( repository, id_job );

      // Load it back into a new job entry
      SqoopImportJobEntry je2 = new SqoopImportJobEntry( mockLogChannelInterface );
      je2.loadRep( repository, id_job, null, null );

      // Make sure all settings we set are properly loaded
      SqoopImportConfig config2 = je2.getJobConfig();
      assertEquals( config.getJobEntryName(), config2.getJobEntryName() );
      assertEquals( config.getBlockingExecution(), config2.getBlockingExecution() );
      assertEquals( config.getBlockingPollingInterval(), config2.getBlockingPollingInterval() );
      assertEquals( config.getConnect(), config2.getConnect() );
      assertEquals( config.getTargetDir(), config2.getTargetDir() );
      assertEquals( config.getHbaseZookeeperQuorum(), config2.getHbaseZookeeperQuorum() );
    } finally {
      // Delete test database
      new File( filename + ".h2.db" ).delete();
      new File( filename + ".trace.db" ).delete();
    }
  }

  @Test
  public void saveLoadTest_rep_advanced_options() throws KettleException, IOException {
    SqoopImportConfig config = new SqoopImportConfig();
    String connectValue = "jdbc:mysql://localhost:3306/test";

    config.setJobEntryName( "testing" );
    config.setBlockingExecution( "false" );
    config.setBlockingPollingInterval( "100" );
    config.setConnect( connectValue );
    config.setTargetDir( "/test-import-target" );
    config.setHbaseZookeeperQuorum( "test-zookeeper-host" );
    config.copyConnectionInfoToAdvanced();

    sqoopImportJobEntry.setJobConfig( config );

    KettleEnvironment.init();
    String filename = File.createTempFile( getClass().getSimpleName() + "-import-dbtest", "" ).getAbsolutePath();

    try {
      DatabaseMeta databaseMeta = new DatabaseMeta( "H2Repo", "H2", "JDBC", null, filename, null, null, null );
      RepositoryMeta repositoryMeta =
          new KettleDatabaseRepositoryMeta( "KettleDatabaseRepository", "H2Repo", "H2 Repository", databaseMeta );
      KettleDatabaseRepository repository = new KettleDatabaseRepository();
      repository.init( repositoryMeta );
      repository.connectionDelegate.connect( true, true );
      KettleDatabaseRepositoryCreationHelper helper = new KettleDatabaseRepositoryCreationHelper( repository );
      helper.createRepositorySchema( null, false, new ArrayList<String>(), false );
      repository.disconnect();

      // Test connecting...
      //
      repository.connect( "admin", "admin" );
      assertTrue( repository.isConnected() );

      // A job entry must have an ID if we're going to save it to a repository
      sqoopImportJobEntry.setObjectId( new LongObjectId( 1 ) );
      ObjectId id_job = new LongObjectId( 1 );

      // Save the original job entry into the repository
      sqoopImportJobEntry.saveRep( repository, id_job );

      // Load it back into a new job entry
      SqoopImportJobEntry je2 = new SqoopImportJobEntry( mockLogChannelInterface );
      je2.loadRep( repository, id_job, null, null );

      // Make sure all settings we set are properly loaded
      SqoopImportConfig config2 = je2.getJobConfig();
      assertEquals( config.getJobEntryName(), config2.getJobEntryName() );
      assertEquals( config.getBlockingExecution(), config2.getBlockingExecution() );
      assertEquals( config.getBlockingPollingInterval(), config2.getBlockingPollingInterval() );
      assertEquals( config.getConnect(), config2.getConnect() );
      assertEquals( config.getTargetDir(), config2.getTargetDir() );
      assertEquals( config.getHbaseZookeeperQuorum(), config2.getHbaseZookeeperQuorum() );

      assertEquals( config.getConnectFromAdvanced(), config2.getConnectFromAdvanced() );
      assertEquals( config.getUsernameFromAdvanced(), config2.getUsernameFromAdvanced() );
      assertEquals( config.getPasswordFromAdvanced(), config2.getPasswordFromAdvanced() );
    } finally {
      // Delete test database
      new File( filename + ".h2.db" ).delete();
      new File( filename + ".trace.db" ).delete();
    }
  }

  @Test
  public void setJobResultFailed() {
    Result jobResult = new Result();
    sqoopImportJobEntry.setJobResultFailed( jobResult );
    assertEquals( 1L, jobResult.getNrErrors() );
    assertFalse( jobResult.getResult() );
  }

  @Test
  public void configure() throws KettleException {
    SqoopImportConfig sqoopConfig = sqoopImportJobEntry.getJobConfig();
    HadoopShim shim = new CommonHadoopShim();
    Configuration conf = shim.createConfiguration();
    DatabaseMeta mockDbMeta = mock( DatabaseMeta.class );
    when( mockParentJobMeta.findDatabase( sqoopConfig.getDatabase() ) ).thenReturn( mockDbMeta );
    try {
      sqoopImportJobEntry.configure( shim, sqoopConfig, conf );
      fail( "Expected exception" );
    } catch ( KettleException ex ) {
      if ( !ex.getMessage().contains( "hdfs host" ) ) {
        ex.printStackTrace();
        fail( "Wrong exception" );
      }
    }

    sqoopImportJobEntry.getJobConfig().setNamenodeHost( "localhost" );
    try {
      sqoopImportJobEntry.configure( shim, sqoopConfig, conf );
      fail( "Expected exception" );
    } catch ( KettleException ex ) {
      if ( !ex.getMessage().contains( "job tracker" ) ) {
        ex.printStackTrace();
        fail( "Wrong exception" );
      }
    }

    sqoopImportJobEntry.getJobConfig().setNamenodePort( "54310" );
    sqoopImportJobEntry.getJobConfig().setJobtrackerHost( "anotherhost" );
    sqoopImportJobEntry.getJobConfig().setJobtrackerPort( "54311" );
    sqoopImportJobEntry.configure( shim, sqoopConfig, conf );

    assertEquals( "localhost", sqoopImportJobEntry.getJobConfig().getNamenodeHost() );
    assertEquals( "54310", sqoopImportJobEntry.getJobConfig().getNamenodePort() );
    assertEquals( "anotherhost", sqoopImportJobEntry.getJobConfig().getJobtrackerHost() );
    assertEquals( "54311", sqoopImportJobEntry.getJobConfig().getJobtrackerPort() );
  }

  @Test
  public void attachAndRemoveLoggingAppenders() {
    PrintStream stderr = System.err;
    Logger sqoopLogger = JobEntryUtils.findLogger( "org.apache.sqoop" );
    Logger hadoopLogger = JobEntryUtils.findLogger( "org.apache.hadoop" );

    assertFalse( sqoopLogger.getAllAppenders().hasMoreElements() );
    assertFalse( hadoopLogger.getAllAppenders().hasMoreElements() );

    try {
      sqoopImportJobEntry.attachLoggingAppenders();

      assertTrue( sqoopLogger.getAllAppenders().hasMoreElements() );
      assertTrue( hadoopLogger.getAllAppenders().hasMoreElements() );

      assertEquals( LoggingProxy.class, System.err.getClass() );

      sqoopImportJobEntry.removeLoggingAppenders();
      assertFalse( sqoopLogger.getAllAppenders().hasMoreElements() );
      assertFalse( hadoopLogger.getAllAppenders().hasMoreElements() );
      assertEquals( stderr, System.err );

    } finally {
      System.setErr( stderr );
      sqoopLogger.removeAllAppenders();
      hadoopLogger.removeAllAppenders();
    }
  }
}
