/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.sqoop;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.big.data.impl.cluster.NamedClusterImpl;
import org.pentaho.big.data.kettle.plugins.job.BlockableJobConfig;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.plugins.DatabasePluginType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.platform.api.util.LogUtil;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class AbstractSqoopJobEntryTest {
  @Mock NamedClusterService mockNamedClusterService;
  @Mock NamedClusterServiceLocator mockNamedClusterServiceLocator;
  @Mock RuntimeTestActionService mockRuntimeTestActionService;
  @Mock RuntimeTester mockRuntimeTester;
  @Mock Job mockJob;
  @Spy JobMeta mockJobMeta;
  @Mock DatabaseMeta mockDatabaseMeta;
  @Mock HadoopClientServices mockHadoopClientServices;
  NamedClusterImpl namedClusterTemplate = new NamedClusterImpl();
  NamedClusterImpl namedClusterActual = new NamedClusterImpl();
  SqoopConfig sqoopConfig;
  AbstractSqoopJobEntry sqoopJobEntry;

  private static final String CLUSTER_NAME_VARIABLE = "ClusterNameVariable";
  private static final String CLUSTER_NAME = "MyClusterName";

  @Before
  public void setUp() throws Exception {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.addPluginType( DatabasePluginType.getInstance() );
    PluginRegistry.init( false );
    Encr.init( "Kettle" );
    KettleLogStore.init();
    namedClusterTemplate.setName( "${" + CLUSTER_NAME_VARIABLE + "}" );
    namedClusterActual.setName( CLUSTER_NAME );

    sqoopJobEntry =
      new AbstractSqoopJobEntry( mockNamedClusterService, mockNamedClusterServiceLocator, mockRuntimeTestActionService,
        mockRuntimeTester ) {

        @Override protected BlockableJobConfig createJobConfig() {
          return null;
        }

        @Override public List<String> getValidationWarnings( BlockableJobConfig config ) {
          return null;
        }

        @Override protected String getToolName() {
          return "toolName";
        }
      };

    sqoopConfig = new SqoopConfig() {
      @Override protected NamedCluster createClusterTemplate() {
        return namedClusterTemplate;
      }
    };

    sqoopConfig.setClusterName( "${" + CLUSTER_NAME_VARIABLE + "}" );
    sqoopConfig.setJobEntryName( "jobname" );
    
    sqoopJobEntry.setParentJob( mockJob );
    VariableSpace variables = new Variables();
    variables.setVariable( CLUSTER_NAME_VARIABLE, CLUSTER_NAME );

    mockJobMeta.initializeVariablesFrom( variables );
    sqoopJobEntry.setParentJobMeta( mockJobMeta );
    when( mockJob.getJobMeta() ).thenReturn( mockJobMeta );
    when( mockJobMeta.findDatabase( any() ) ).thenReturn( mockDatabaseMeta );
    when( mockNamedClusterService.contains( eq( CLUSTER_NAME ), any() ) ).thenReturn( true );
    when( mockNamedClusterService.read( eq( CLUSTER_NAME ), any() ) ).thenReturn( namedClusterActual );

    when( mockNamedClusterServiceLocator.getService( any(), any() ) ).thenReturn( mockHadoopClientServices );
    when( mockHadoopClientServices.runSqoop( any(), any() ) ).thenReturn( 0 );
  }

  @Test
  public void executeSqoopWithVariableClusterName() throws Exception {

    sqoopJobEntry.setJobConfig( sqoopConfig );
    Result jobResult = new Result();

    sqoopJobEntry.executeSqoop( jobResult );
    verify( mockNamedClusterService, times( 1 ) ).read( any(), any() );
  }

  @Test
  public void keepsSystemErrorRedirectedUntilAllConcurrentExecutionsFinish() {
    AbstractSqoopJobEntry secondJobEntry = createJobEntry();
    PrintStream originalSystemError = System.err;

    try {
      sqoopJobEntry.attachLoggingAppenders();
      secondJobEntry.attachLoggingAppenders();

      sqoopJobEntry.removeLoggingAppenders();
      assertNotSame( originalSystemError, System.err );

      secondJobEntry.removeLoggingAppenders();
      assertSame( originalSystemError, System.err );
    } finally {
      sqoopJobEntry.removeLoggingAppenders();
      secondJobEntry.removeLoggingAppenders();
    }
  }

  @Test
  public void attachesLoggingAppendersOnlyOnce() {
    Logger sqoopLogger = LogManager.getLogger( "org.apache.sqoop" );
    int initialAppenderCount = LogUtil.getAppenders( sqoopLogger ).size();
    PrintStream originalSystemError = System.err;

    try {
      sqoopJobEntry.attachLoggingAppenders();
      sqoopJobEntry.attachLoggingAppenders();

      sqoopJobEntry.removeLoggingAppenders();

      assertSame( originalSystemError, System.err );
      assertEquals( initialAppenderCount, LogUtil.getAppenders( sqoopLogger ).size() );
    } finally {
      sqoopJobEntry.removeLoggingAppenders();
    }
  }

  @Test
  public void attachesLoggingAppendersToShimLoggerContext() throws Exception {
    try ( URLClassLoader shimClassLoader = new URLClassLoader( new URL[ 0 ], getClass().getClassLoader() ) ) {
      LoggerContext shimLoggerContext = ( LoggerContext ) LogManager.getContext( shimClassLoader, false );
      org.apache.logging.log4j.core.Logger hadoopLogger = shimLoggerContext.getLogger( "org.apache.hadoop" );
      Set<Appender> existingAppenders = new HashSet<>( hadoopLogger.getAppenders().values() );

      sqoopJobEntry.attachLoggingAppenders( shimClassLoader );
      try {
        Set<Appender> addedAppenders = new HashSet<>( hadoopLogger.getAppenders().values() );
        addedAppenders.removeAll( existingAppenders );

        assertTrue( addedAppenders.stream().anyMatch(
          appender -> appender instanceof org.pentaho.di.core.logging.log4j.KettleLogChannelAppender ) );
      } finally {
        sqoopJobEntry.removeLoggingAppenders();
      }
    }
  }

  @Test
  public void redirectsSystemErrorToTheLoggerOfTheOwningExecution() throws Exception {
    AbstractSqoopJobEntry shimJobEntry = createJobEntry();
    // a parent-less loader gets its own LoggerContext, like a shim bundle classloader does
    try ( URLClassLoader shimClassLoader = new URLClassLoader( new URL[ 0 ], null ) ) {
      LoggerContext shimLoggerContext = ( LoggerContext ) LogManager.getContext( shimClassLoader, false );
      assertNotSame( LogManager.getContext( false ), shimLoggerContext );
      org.apache.logging.log4j.core.Logger shimSqoopLogger = shimLoggerContext.getLogger( "org.apache.sqoop" );
      CapturingAppender capturingAppender = new CapturingAppender();
      capturingAppender.start();

      // the first execution installs the JVM-wide System.err proxy, the second one runs in another logger context
      sqoopJobEntry.attachLoggingAppenders();
      shimJobEntry.attachLoggingAppenders( shimClassLoader );
      shimSqoopLogger.addAppender( capturingAppender );
      shimSqoopLogger.setLevel( Level.INFO );
      try {
        System.err.print( "sqoop wrote this to stderr" );

        assertTrue( capturingAppender.messages.contains( "sqoop wrote this to stderr" ) );
      } finally {
        shimSqoopLogger.removeAppender( capturingAppender );
        shimJobEntry.removeLoggingAppenders();
        sqoopJobEntry.removeLoggingAppenders();
      }
    }
  }

  /** Concurrent Sqoop executions keep Hadoop, Sqoop, and stderr messages separate, including after one exits. */
  @Test
  public void keepsConcurrentSqoopExecutionLogsSeparate() throws Exception {
    AbstractSqoopJobEntry firstEntry = createJobEntry( "sqoop-first" );
    AbstractSqoopJobEntry secondEntry = createJobEntry( "sqoop-second" );
    assertNotEquals( firstEntry.getLogChannelId(), secondEntry.getLogChannelId() );
    CountDownLatch attached = new CountDownLatch( 2 );
    CountDownLatch logged = new CountDownLatch( 2 );
    CountDownLatch firstFinished = new CountDownLatch( 1 );
    ExecutorService executor = Executors.newFixedThreadPool( 2 );

    try {
      List<Future<?>> executions = new ArrayList<>();
      for ( AbstractSqoopJobEntry entry : List.of( firstEntry, secondEntry ) ) {
        String marker = entry == firstEntry ? "first-execution" : "second-execution";
        executions.add( executor.submit( () -> {
          entry.attachLoggingAppenders();
          try {
            attached.countDown();
            assertTrue( "Both executions must attach before logging", attached.await( 10, TimeUnit.SECONDS ) );
            LogManager.getLogger( "org.apache.sqoop" ).info( marker + " sqoop" );
            LogManager.getLogger( "org.apache.hadoop" ).info( marker + " File System Counters Job Counters" );
            System.err.println( marker + " stderr" );
            logged.countDown();
            assertTrue( "Both executions must log before detaching", logged.await( 10, TimeUnit.SECONDS ) );
            if ( entry == secondEntry ) {
              assertTrue( "First execution must finish", firstFinished.await( 10, TimeUnit.SECONDS ) );
              LogManager.getLogger( "org.apache.hadoop" ).info( marker + " after-first-finished" );
              System.err.println( marker + " stderr-after-first-finished" );
            }
          } finally {
            entry.removeLoggingAppenders();
            if ( entry == firstEntry ) {
              firstFinished.countDown();
            }
          }
          return null;
        } ) );
      }
      for ( Future<?> execution : executions ) {
        execution.get( 30, TimeUnit.SECONDS );
      }
    } finally {
      executor.shutdownNow();
      assertTrue( "Execution threads must terminate", executor.awaitTermination( 10, TimeUnit.SECONDS ) );
    }

    String firstLog = KettleLogStore.getAppender().getBuffer( firstEntry.getLogChannelId(), false ).toString();
    String secondLog = KettleLogStore.getAppender().getBuffer( secondEntry.getLogChannelId(), false ).toString();
    for ( String kind : List.of( "sqoop", "File System Counters Job Counters", "stderr" ) ) {
      assertTrue( firstLog, firstLog.contains( "first-execution " + kind ) );
      assertTrue( secondLog, secondLog.contains( "second-execution " + kind ) );
    }
    assertFalse( firstLog, firstLog.contains( "second-execution" ) );
    assertFalse( secondLog, secondLog.contains( "first-execution" ) );
    assertTrue( secondLog, secondLog.contains( "second-execution after-first-finished" ) );
    assertTrue( secondLog, secondLog.contains( "second-execution stderr-after-first-finished" ) );
  }

  /** Creates an entry using its default log channel. */
  private AbstractSqoopJobEntry createJobEntry() {
    return createJobEntry( null );
  }

  /** Creates an entry with an optional distinct Kettle log channel for concurrent logging tests. */
  private AbstractSqoopJobEntry createJobEntry( String channelName ) {
    return new AbstractSqoopJobEntry( mockNamedClusterService, mockNamedClusterServiceLocator,
      mockRuntimeTestActionService, mockRuntimeTester ) {
      {
        if ( channelName != null ) {
          log = new LogChannel( channelName );
        }
      }

      @Override protected BlockableJobConfig createJobConfig() {
        return null;
      }

      @Override public List<String> getValidationWarnings( BlockableJobConfig config ) {
        return null;
      }

      @Override protected String getToolName() {
        return "toolName";
      }
    };
  }

  private static final class CapturingAppender extends AbstractAppender {
    private final List<String> messages = new CopyOnWriteArrayList<>();

    private CapturingAppender() {
      super( "capturing-appender", null, null, true, Property.EMPTY_ARRAY );
    }

    @Override public void append( LogEvent event ) {
      messages.add( event.getMessage().getFormattedMessage() );
    }
  }

}