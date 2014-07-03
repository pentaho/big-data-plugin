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

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.HiveDatabaseMeta;
import org.pentaho.di.core.database.MySQLDatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;

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
}
