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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.HiveDatabaseMeta;
import org.pentaho.di.core.database.MySQLDatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobEntryMode;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

public class AbstractSqoopJobEntryTest {
  private LogChannelInterface mockLogChannelInterface;

  @Before
  public void setup() {
    mockLogChannelInterface = mock( LogChannelInterface.class );
  }

  private static class TestSqoopJobEntry extends AbstractSqoopJobEntry<SqoopConfig> {
    private long waitTime = 0L;

    /**
     * Create a {@link SqoopImportJobEntry} that will simply wait for {@code waitTime} instead of executing Sqoop.
     * 
     * @param waitTime
     *          Time in milliseconds to block during
     *          {@link AbstractSqoopJobEntry#executeSqoop(SqoopConfig, org.apache.hadoop.conf.Configuration, org.pentaho.di.core.Result)}
     */
    private TestSqoopJobEntry( long waitTime, LogChannelInterface logChannelInterface ) {
      super( logChannelInterface );
      this.waitTime = waitTime;
    }

    @Override
    protected void executeSqoop( HadoopShim hadoopShim, SqoopShim sqoopShim, SqoopConfig config,
        Configuration hadoopConfig, Result jobResult ) {
      // Don't actually execute sqoop, just block for the requested time
      try {
        Thread.sleep( waitTime );
      } catch ( InterruptedException e ) {
        throw new RuntimeException( e );
      }
    }

    @Override
    protected SqoopConfig buildSqoopConfig() {
      SqoopConfig config = new SqoopConfig() {
      };
      config.setConnect( "jdbc:bogus://localhost" );
      return config;
    }

    @Override
    protected String getToolName() {
      return null;
    }
  }

  @Test
  public void execute_invalid_config() throws KettleException {
    final List<String> loggedErrors = new ArrayList<String>();
    AbstractSqoopJobEntry<SqoopConfig> je = new TestSqoopJobEntry( 0, mockLogChannelInterface ) {
      @Override
      public void logError( String message ) {
        loggedErrors.add( message );
      }
    };
    je.getJobConfig().setConnect( null );

    Result result = new Result();
    je.execute( result, 0 );

    assertEquals( 1, loggedErrors.size() );
    assertEquals( 1, result.getNrErrors() );
    assertFalse( result.getResult() );
  }

  //
  // @Test
  // public void execute_blocking() throws KettleException {
  // final long waitTime = 1000;
  // AbstractSqoopJobEntry je = new TestSqoopJobEntry(waitTime);
  //
  // je.setParentJob(new Job("test", null, null));
  // Result result = new Result();
  // long start = System.currentTimeMillis();
  // je.execute(result, 0);
  // long end = System.currentTimeMillis();
  // assertTrue("Total runtime should be >= the wait time if we are blocking", (end - start) >= waitTime);
  //
  // assertEquals(0, result.getNrErrors());
  // assertTrue(result.getResult());
  // }
  //
  // @Test
  // public void execute_nonblocking() throws KettleException {
  // final long waitTime = 1000;
  // AbstractSqoopJobEntry<SqoopConfig> je = new TestSqoopJobEntry(waitTime);
  //
  // je.setParentJob(new Job("test", null, null));
  // je.getJobConfig().setBlockingExecution("false");
  // Result result = new Result();
  // long start = System.currentTimeMillis();
  // je.execute(result, 0);
  // long end = System.currentTimeMillis();
  // assertTrue("Total runtime should be less than the wait time if we're not blocking", (end - start) < waitTime);
  //
  // assertEquals(0, result.getNrErrors());
  // assertTrue(result.getResult());
  // }
  //
  // @Test
  // public void execute_interrupted() throws KettleException {
  // final long waitTime = 1000 * 10;
  // final List<String> loggedErrors = new ArrayList<String>();
  // AbstractSqoopJobEntry je = new TestSqoopJobEntry(waitTime) {
  // @Override
  // public void logError(String message, Throwable e) {
  // loggedErrors.add(message);
  // }
  // };
  //
  // final Job parentJob = new Job("test", null, null);
  //
  // Thread t = new Thread() {
  // @Override
  // public void run() {
  // try {
  // Thread.sleep(1000);
  // } catch (InterruptedException e) {
  // throw new RuntimeException(e);
  // }
  // parentJob.stopAll();
  // }
  // };
  //
  // je.setParentJob(parentJob);
  // Result result = new Result();
  //
  // // Start another thread to stop the parent job and unblock the Sqoop job entry in 1 second
  // t.start();
  //
  // long start = System.currentTimeMillis();
  // je.execute(result, 0);
  // long end = System.currentTimeMillis();
  // assertTrue("Total runtime should be less than the wait time if we were properly interrupted", (end - start) <
  // waitTime);
  //
  // assertEquals(1, result.getNrErrors());
  // assertFalse(result.getResult());
  //
  // // Make sure when an uncaught exception occurs an error log message is generated
  // assertEquals(1, loggedErrors.size());
  // }

  @Test
  public void isValidSqoopConfig_connect() {
    SqoopConfig config = new SqoopConfig() {
    };

    final List<String> errorsLogged = new ArrayList<String>();
    AbstractSqoopJobEntry jobEntry = new TestSqoopJobEntry( 10, mockLogChannelInterface ) {
      @Override
      public void logError( String message ) {
        errorsLogged.add( message );
      }
    };

    assertFalse( jobEntry.isValid( config ) );
    assertEquals( 1, errorsLogged.size() );
    errorsLogged.clear();

    config.setConnect( "bogus but not empty" );
    assertTrue( jobEntry.isValid( config ) );
    assertEquals( 0, errorsLogged.size() );

    config.setBlockingPollingInterval( "asdf" );
    assertFalse( jobEntry.isValid( config ) );
    assertEquals( 1, errorsLogged.size() );
  }

  @Test
  public void isDatabaseSupported() {
    AbstractSqoopJobEntry jobEntry = new TestSqoopJobEntry( 10, mockLogChannelInterface );

    assertTrue( jobEntry.isDatabaseSupported( MySQLDatabaseMeta.class ) );
    // All database are "supported" for now
    assertTrue( jobEntry.isDatabaseSupported( HiveDatabaseMeta.class ) );
  }

  @Test
  public void configureDatabaseTest() throws Exception {
    final String DBNAME = "dbname";
    final String URI = "jdbc:dbtype://host/port?param";
    final String USER = "user";
    final String PASSWORD = "password";
    final String SUFFIX = "_FROM_META";

    AbstractSqoopJobEntry<SqoopConfig> jobEntry = new TestSqoopJobEntry( 10, mockLogChannelInterface ) {
      @Override
      public Job getParentJob() {
        DatabaseMeta dbMeta = mock( DatabaseMeta.class );
        when( dbMeta.getName() ).thenReturn( DBNAME + SUFFIX );
        try {
          when( dbMeta.getURL() ).thenReturn( URI + SUFFIX );
        } catch ( KettleException e ) {
          throw new RuntimeException( "Rethrow out", e );
        }
        when( dbMeta.getUsername() ).thenReturn( USER + SUFFIX );
        when( dbMeta.getPassword() ).thenReturn( PASSWORD + SUFFIX );

        Job job = mock( Job.class );
        JobMeta jmeta = mock( JobMeta.class );

        when( jmeta.findDatabase( DBNAME + SUFFIX ) ).thenReturn( dbMeta );
        when( jmeta.findDatabase( DBNAME ) ).thenReturn( null );
        when( job.getJobMeta() ).thenReturn( jmeta );
        return job;
      }
    };

    SqoopConfig conf = new SqoopConfig() { };
    conf.setConnectionInfo( DBNAME, URI, USER, PASSWORD );

    conf.setMode( JobEntryMode.ADVANCED_COMMAND_LINE );
    jobEntry.configureDatabase( conf );

    assertEquals( "database should not be changed in ADVANCED_COMMAND_LINE mode", DBNAME, conf.getDatabase() );
    assertEquals( "uri should not be changed in ADVANCED_COMMAND_LINE mode",      URI, conf.getConnect() );
    assertEquals( "user should not be changed in ADVANCED_COMMAND_LINE mode",     USER, conf.getUsername() );
    assertEquals( "password should not be changed in ADVANCED_COMMAND_LINE mode", PASSWORD, conf.getPassword() );

    conf.setMode( JobEntryMode.ADVANCED_LIST );
    jobEntry.configureDatabase( conf );

    assertEquals( "database should not be changed in ADVANCED_LIST mode", DBNAME, conf.getDatabase() );
    assertEquals( "uri should not be changed in ADVANCED_LIST mode",      URI, conf.getConnect() );
    assertEquals( "user should not be changed in ADVANCED_LIST mode",     USER, conf.getUsername() );
    assertEquals( "password should not be changed in ADVANCED_LIST mode", PASSWORD, conf.getPassword() );

    conf.setMode( JobEntryMode.QUICK_SETUP );
    jobEntry.configureDatabase( conf );

    assertEquals( "database should not be changed in ADVANCED_LIST mode", DBNAME, conf.getDatabase() );
    assertEquals( "uri should not be changed in ADVANCED_LIST mode",      URI, conf.getConnect() );
    assertEquals( "user should not be changed in ADVANCED_LIST mode",     USER, conf.getUsername() );
    assertEquals( "password should not be changed in ADVANCED_LIST mode", PASSWORD, conf.getPassword() );

    conf.setMode( JobEntryMode.QUICK_SETUP );
    conf.setDatabase( DBNAME + SUFFIX );
    jobEntry.configureDatabase( conf );

    assertEquals( "database should be as DBMeta in QUICK_SETUP mode", DBNAME + SUFFIX, conf.getDatabase() );
    assertEquals( "uri should be as DBMeta in QUICK_SETUP mode",      URI + SUFFIX, conf.getConnect() );
    assertEquals( "user should be as DBMeta in QUICK_SETUP mode",     USER + SUFFIX, conf.getUsername() );
    assertEquals( "password should be as DBMeta in QUICK_SETUP mode", PASSWORD + SUFFIX, conf.getPassword() );
  }

  @Test
  public void createJobConfigTest() throws Exception {
    final String NNH = "nameNodeHost";
    final String NNP = "8020";
    final String JTH = "jobTrackerHost";
    final String JTP = "8232";

    final HadoopShim shim = mock( HadoopShim.class );
    when( shim.getNamenodeConnectionInfo( any( Configuration.class ) ) )
      .thenReturn( new String[] { NNH, NNP } );
    when( shim.getJobtrackerConnectionInfo( any( Configuration.class ) ) )
      .thenReturn( new String[] { JTH, JTP } );

    final HadoopConfiguration hc = mock(HadoopConfiguration.class );
    when( hc.getHadoopShim( ) ).thenReturn( shim );

    AbstractSqoopJobEntry<SqoopConfig> jobEntry = new TestSqoopJobEntry( 10, mockLogChannelInterface ) {
      @Override
      protected HadoopConfiguration getActiveHadoopConfiguration() throws ConfigurationException {
        return hc;
      }
    };

    SqoopConfig config = jobEntry.createJobConfig();

    assertEquals( "name-node-host check failed", NNH, config.getNamenodeHost() );
    assertEquals( "name-node-port check failed", NNP, config.getNamenodePort() );

    assertEquals( "job-tracker-host check failed", JTH, config.getJobtrackerHost() );
    assertEquals( "job-tracker-port check failed", JTP, config.getJobtrackerPort() );
  }

  @Test
  public void loadNamedClusterTest_no_rep() throws Exception {
    AbstractSqoopJobEntry<SqoopConfig> jobEntry = new TestSqoopJobEntry( 10, mockLogChannelInterface );
    NamedCluster nc = new NamedCluster();
    SqoopConfig config = new SqoopConfig() { };
    config.setNamedCluster( nc );

    NamedCluster res = jobEntry.loadNamedCluster( config );

    assertEquals( "if rep is not set the internally saved NC should be returned", nc, res );
  }

  @Test
  public void loadNamedClusterTest_with_rep() throws Exception {
    final String NAME = "special_name_for named cluster";

    NamedCluster nc = new NamedCluster();
    nc.setName( NAME );
    IMetaStore ms = new MemoryMetaStore();
    NamedClusterManager.getInstance().create( nc, ms );

    nc.setName( "this is another name to distiguish from one saved in the rep" );

    Repository rep = mock( Repository.class );
    when( rep.getMetaStore() ).thenReturn( ms );

    SqoopConfig config = new SqoopConfig() { };
    config.setNamedCluster( nc );
    config.setClusterName( NAME );

    AbstractSqoopJobEntry<SqoopConfig> jobEntry = new TestSqoopJobEntry( 10, mockLogChannelInterface );
    jobEntry.setRepository( rep );

    NamedCluster res = jobEntry.loadNamedCluster( config );

    assertEquals( "if rep is set the NC from the rep should be returned", NAME, res.getName() );
  }
}
