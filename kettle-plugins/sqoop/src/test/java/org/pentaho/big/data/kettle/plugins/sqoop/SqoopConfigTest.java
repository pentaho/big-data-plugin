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

package org.pentaho.big.data.kettle.plugins.sqoop;

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.big.data.kettle.plugins.job.JobEntryMode;
import org.pentaho.big.data.kettle.plugins.job.PropertyEntry;
import org.pentaho.big.data.kettle.plugins.sqoop.util.MockitoAutoBean;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.ui.xul.util.AbstractModelList;
import org.w3c.dom.Node;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the SqoopConfig functionality not exercised by {@link PropertyFiringObjectTest}
 */
@RunWith( MockitoJUnitRunner.class )
public class SqoopConfigTest {
  /**
   *
   */
  private static final String ENTRY_VALUE = "entryValue";
  /**
   *
   */
  private static final String ENTRY_KEY = "entryKey";
  private static final String EMPTY = "";
  public static final String HDFS_HOST = "hdfsHost";
  public static final String HDFS_PORT = "8020";
  public static final String JOB_TRACKER_HOST = "jobTracker";
  public static final String JOB_TRACKER_PORT = "2222";
  private NamedCluster namedClusterMock = mock( NamedCluster.class );

  private SqoopConfig config;
  private NamedCluster template;
  @Mock Runnable createClusterTemplate;

  @Before
  public void setUp() throws Exception {
    config = createSqoopConfig();
  }

  protected SqoopConfig createSqoopConfig() {
    return new SqoopConfig() {
      @Override protected NamedCluster createClusterTemplate() {
        template = mock( NamedCluster.class );

        MockitoAutoBean<String> hdfsHost = new MockitoAutoBean<>( HDFS_HOST );
        doAnswer( hdfsHost ).when( template ).getHdfsHost();
        doAnswer( hdfsHost ).when( template ).setHdfsHost( anyString() );

        MockitoAutoBean<String> hdfsPort = new MockitoAutoBean<>( HDFS_PORT );
        doAnswer( hdfsPort ).when( template ).getHdfsPort();
        doAnswer( hdfsPort ).when( template ).setHdfsPort( anyString() );

        MockitoAutoBean<String> jobTrackerHost = new MockitoAutoBean<>( JOB_TRACKER_HOST );
        doAnswer( jobTrackerHost ).when( template ).getJobTrackerHost();
        doAnswer( jobTrackerHost ).when( template ).setJobTrackerHost( anyString() );

        MockitoAutoBean<String> jobTrackerPort = new MockitoAutoBean<>( JOB_TRACKER_PORT );
        doAnswer( jobTrackerPort ).when( template ).getJobTrackerPort();
        doAnswer( jobTrackerPort ).when( template ).setJobTrackerPort( anyString() );

        createClusterTemplate.run();
        return template;
      }
    };
  }

  @Test
  public void addRemovePropertyChangeListener() {
    PersistentPropertyChangeListener l = new PersistentPropertyChangeListener();

    config.addPropertyChangeListener( l );
    config.setJobEntryName( "test" );
    assertEquals( 1, l.getReceivedEvents().size() );
    config.removePropertyChangeListener( l );
    config.setJobEntryName( "test1" );
    assertEquals( 1, l.getReceivedEvents().size() );
  }

  @Test
  public void addRemovePropertyChangeListener_propertyName() {
    PersistentPropertyChangeListener l = new PersistentPropertyChangeListener();

    config.addPropertyChangeListener( "test", l );
    config.setJobEntryName( "test" );
    assertEquals( 0, l.getReceivedEvents().size() );
    config.removePropertyChangeListener( "test", l );

    config.addPropertyChangeListener( SqoopConfig.JOB_ENTRY_NAME, l );
    config.setJobEntryName( "test1" );
    assertEquals( 1, l.getReceivedEvents().size() );
    config.removePropertyChangeListener( SqoopConfig.JOB_ENTRY_NAME, l );
    config.setJobEntryName( "test2" );
    assertEquals( 1, l.getReceivedEvents().size() );
  }

  @Test
  public void getAdvancedArgumentsList() {
    AbstractModelList<ArgumentWrapper> args = config.getAdvancedArgumentsList();
    assertEquals( 59, args.size() );

    PropertyChangeListener l = mock( PropertyChangeListener.class );
    config.addPropertyChangeListener( l );

    // Make sure we can get and set the value for all arguments returned
    String value = String.valueOf( System.currentTimeMillis() );
    for ( ArgumentWrapper arg : args ) {
      arg.setValue( value );
      assertEquals( value, arg.getValue() );
    }

    // We should have received one event for every property changed
    verify( l, times( 59 ) ).propertyChange( (PropertyChangeEvent) any() );
  }

  @Test
  public void testClone() {
    config.setConnect( SqoopConfig.CONNECT );
    config.setJobEntryName( SqoopConfig.JOB_ENTRY_NAME );

    SqoopConfig clone = config.clone();

    assertEquals( config.getConnect(), clone.getConnect() );
    assertEquals( config.getJobEntryName(), clone.getJobEntryName() );
  }

  @Test
  public void setDatabaseConnectionInformation() {
    PersistentPropertyChangeListener l = new PersistentPropertyChangeListener();
    config.addPropertyChangeListener( l );

    String database = "bogus";
    String connect = "jdbc:bogus://bogus";
    String username = "bob";
    String password = "uncle";

    config.setConnectionInfo( database, connect, username, password );

    assertEquals( 0, l.getReceivedEvents().size() );
    assertEquals( database, config.getDatabase() );
    assertEquals( connect, config.getConnect() );
    assertEquals( username, config.getUsername() );
    assertEquals( password, config.getPassword() );
  }

  @Test
  public void numMappers() {
    String numMappers = "5";

    config.setNumMappers( numMappers );

    List<String> args = new ArrayList<String>();

    List<ArgumentWrapper> argumentWrappers = config.getAdvancedArgumentsList();

    ArgumentWrapper arg = null;
    Iterator<ArgumentWrapper> argIter = argumentWrappers.iterator();
    while ( arg == null && argIter.hasNext() ) {
      ArgumentWrapper a = argIter.next();
      if ( a.getName().equals( "num-mappers" ) ) {
        arg = a;
      }
    }
    assertNotNull( arg );
    SqoopUtils.appendArgument( args, arg, new Variables() );
    assertEquals( 2, args.size() );
    assertEquals( "--num-mappers", args.get( 0 ) );
    assertEquals( numMappers, args.get( 1 ) );
  }

  @Test
  public void copyConnectionInfoFromAdvanced() {
    PersistentPropertyChangeListener l = new PersistentPropertyChangeListener();
    config.addPropertyChangeListener( l );

    String connect = "connect";
    String username = "username";
    String password = "password";

    config.setConnectFromAdvanced( connect );
    config.setUsernameFromAdvanced( username );
    config.setPasswordFromAdvanced( password );

    assertNull( config.getConnect() );
    assertNull( config.getUsername() );
    assertNull( config.getPassword() );

    config.copyConnectionInfoFromAdvanced();

    assertEquals( connect, config.getConnect() );
    assertEquals( username, config.getUsername() );
    assertEquals( password, config.getPassword() );

    assertEquals( 0, l.getReceivedEvents().size() );
  }

  @Test
  public void copyConnectionInfoToAdvanced() {
    PersistentPropertyChangeListener l = new PersistentPropertyChangeListener();
    config.addPropertyChangeListener( l );

    String connect = "connect";
    String username = "username";
    String password = "password";

    config.setConnect( connect );
    config.setUsername( username );
    config.setPassword( password );

    assertNull( config.getConnectFromAdvanced() );
    assertNull( config.getUsernameFromAdvanced() );
    assertNull( config.getPasswordFromAdvanced() );

    config.copyConnectionInfoToAdvanced();

    assertEquals( connect, config.getConnectFromAdvanced() );
    assertEquals( username, config.getUsernameFromAdvanced() );
    assertEquals( password, config.getPasswordFromAdvanced() );

    assertEquals( 3, l.getReceivedEvents().size() );
    assertEquals( "connect", l.getReceivedEvents().get( 0 ).getPropertyName() );
    assertEquals( "username", l.getReceivedEvents().get( 1 ).getPropertyName() );
    assertEquals( "password", l.getReceivedEvents().get( 2 ).getPropertyName() );
  }

  @Test
  public void getModeAsEnum() {
    assertNull( config.getMode() );
    assertEquals( JobEntryMode.QUICK_SETUP, config.getModeAsEnum() );

    config.setMode( JobEntryMode.ADVANCED_COMMAND_LINE.name() );
    assertEquals( JobEntryMode.ADVANCED_COMMAND_LINE.name(), config.getMode() );
    assertEquals( JobEntryMode.ADVANCED_COMMAND_LINE, config.getModeAsEnum() );
  }

  @Test
  public void testClusterConfigToXML() throws Exception {
    String xml = "<test>" + config.getClusterXML() + "</test>";
    Node node = XMLHandler.loadXMLString( xml, "test" );

    reset( createClusterTemplate );
    createSqoopConfig().loadClusterConfig( node );

    verify( createClusterTemplate ).run();
    verify( template ).setHdfsHost( HDFS_HOST );
    verify( template ).setHdfsPort( HDFS_PORT );
    verify( template ).setJobTrackerHost( JOB_TRACKER_HOST );
    verify( template ).setJobTrackerPort( JOB_TRACKER_PORT );
  }

  @Test
  public void testClusterConfigToRepo() throws Exception {
    Repository repository = mock( Repository.class );
    StringObjectId id_job = new StringObjectId( UUID.randomUUID().toString() );
    JobEntryInterface jobEntryInterface = mock( JobEntryInterface.class );
    StringObjectId objectId = new StringObjectId( UUID.randomUUID().toString() );
    when( jobEntryInterface.getObjectId() ).thenReturn( objectId );

    final HashMap<String, String> properties = Maps.newHashMap();
    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        Object[] arguments = invocation.getArguments();
        properties.put( ( (String) arguments[2] ), (String) arguments[3] );
        return null;
      }
    } ).when( repository ).saveJobEntryAttribute( eq( id_job ), eq( objectId ), anyString(), anyString() );
    doAnswer( new Answer() {
      @Override public String answer( InvocationOnMock invocation ) throws Throwable {
        return properties.get( invocation.getArguments()[1].toString() );
      }
    } ).when( repository ).getJobEntryAttributeString( eq( id_job ), anyString() );

    config.saveClusterConfig( repository, id_job, jobEntryInterface );
    verify( repository, times( 4 ) ).saveJobEntryAttribute( eq( id_job ), eq( objectId ), anyString(), anyString() );

    reset( createClusterTemplate );
    createSqoopConfig().loadClusterConfig( repository, id_job );

    verify( createClusterTemplate ).run();
    verify( template ).setHdfsHost( HDFS_HOST );
    verify( template ).setHdfsPort( HDFS_PORT );
    verify( template ).setJobTrackerHost( JOB_TRACKER_HOST );
    verify( template ).setJobTrackerPort( JOB_TRACKER_PORT );
  }

  @Test
  public void testSetNamedCluster() throws Exception {
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.getName() ).thenReturn( "named cluster" );
    config.setNamedCluster( namedCluster );

    verify( createClusterTemplate ).run();
    verify( template ).replaceMeta( namedCluster );
    assertEquals( "named cluster", config.getClusterName() );
  }

  @Test
  public void testIsAdvancedClusterConfigSet_ClusterNameNull() throws Exception {
    when( namedClusterMock.getName() ).thenReturn( null );
    config.setNamedCluster( namedClusterMock );
    assertTrue( config.isAdvancedClusterConfigSet() );
  }

  @Test
  public void testIsAdvancedClusterConfigSet_ClusterNameEmpty() throws Exception {
    when( namedClusterMock.getName() ).thenReturn( EMPTY );
    config.setNamedCluster( namedClusterMock );
    assertTrue( config.isAdvancedClusterConfigSet() );
  }

  @Test
  public void testIsAdvancedClusterConfigSet_ClusterNameEmptyOrNullAndAllNcPropertiesNull() throws Exception {
    when( namedClusterMock.getName() ).thenReturn( null );
    config.setNamedCluster( namedClusterMock );
    when( config.getNamedCluster().getHdfsHost() ).thenReturn( null );
    when( config.getNamedCluster().getHdfsPort() ).thenReturn( null );
    when( config.getNamedCluster().getJobTrackerHost() ).thenReturn( null );
    when( config.getNamedCluster().getJobTrackerPort() ).thenReturn( null );
    assertFalse( config.isAdvancedClusterConfigSet() );
  }

  @Test
  public void testIsAdvancedClusterConfigSet_ClusterNameNotNull() throws Exception {
    when( namedClusterMock.getName() ).thenReturn( "Cluster Name For Testing" );
    config.setNamedCluster( namedClusterMock );
    assertFalse( config.isAdvancedClusterConfigSet() );
  }

  @Test
  public void testNcPropertiesNotNullOrEmpty_AllNotNullNotEmpty() throws Exception {
    boolean ncPropertiesNotNullOrEmpty = config.ncPropertiesNotNullOrEmpty( config.getNamedCluster() );
    assertTrue( ncPropertiesNotNullOrEmpty );
  }

  @Test
  public void testNcPropertiesNotNullOrEmpty_AllNull() throws Exception {
    when( config.getNamedCluster().getHdfsHost() ).thenReturn( null );
    when( config.getNamedCluster().getHdfsPort() ).thenReturn( null );
    when( config.getNamedCluster().getJobTrackerHost() ).thenReturn( null );
    when( config.getNamedCluster().getJobTrackerPort() ).thenReturn( null );
    boolean ncPropertiesNotNullOrEmpty = config.ncPropertiesNotNullOrEmpty( config.getNamedCluster() );
    assertFalse( ncPropertiesNotNullOrEmpty );
  }

  @Test
  public void testNcPropertiesNotNullOrEmpty_AllEmpty() throws Exception {
    when( config.getNamedCluster().getHdfsHost() ).thenReturn( EMPTY );
    when( config.getNamedCluster().getHdfsPort() ).thenReturn( EMPTY );
    when( config.getNamedCluster().getJobTrackerHost() ).thenReturn( EMPTY );
    when( config.getNamedCluster().getJobTrackerPort() ).thenReturn( EMPTY );
    boolean ncPropertiesNotNullOrEmpty = config.ncPropertiesNotNullOrEmpty( config.getNamedCluster() );
    assertFalse( ncPropertiesNotNullOrEmpty );
  }

  @Test
  public void testNcPropertiesNotNullOrEmpty_HdfsHostOnlyNotNull() throws Exception {
    when( config.getNamedCluster().getHdfsPort() ).thenReturn( null );
    when( config.getNamedCluster().getJobTrackerHost() ).thenReturn( null );
    when( config.getNamedCluster().getJobTrackerPort() ).thenReturn( null );
    boolean ncPropertiesNotNullOrEmpty = config.ncPropertiesNotNullOrEmpty( config.getNamedCluster() );
    assertTrue( "It should be true - HDFS host: " + config.getNamedCluster().getHdfsHost(), ncPropertiesNotNullOrEmpty );
  }

  @Test
  public void testNcPropertiesNotNullOrEmpty_HdfsPortOnlyNotNull() throws Exception {
    when( config.getNamedCluster().getHdfsHost() ).thenReturn( null );
    when( config.getNamedCluster().getJobTrackerHost() ).thenReturn( null );
    when( config.getNamedCluster().getJobTrackerPort() ).thenReturn( null );
    boolean ncPropertiesNotNullOrEmpty = config.ncPropertiesNotNullOrEmpty( config.getNamedCluster() );
    assertTrue( "It should be true - HDFS port: " + config.getNamedCluster().getHdfsPort(), ncPropertiesNotNullOrEmpty );
  }

  @Test
  public void testNcPropertiesNotNullOrEmpty_JobTrackerHostOnlyNotNull() throws Exception {
    when( config.getNamedCluster().getHdfsHost() ).thenReturn( null );
    when( config.getNamedCluster().getHdfsPort() ).thenReturn( null );
    when( config.getNamedCluster().getJobTrackerPort() ).thenReturn( null );
    boolean ncPropertiesNotNullOrEmpty = config.ncPropertiesNotNullOrEmpty( config.getNamedCluster() );
    assertTrue( "It should be true - Job tracker host: " + config.getNamedCluster().getJobTrackerHost(), ncPropertiesNotNullOrEmpty );
  }

  @Test
  public void testNcPropertiesNotNullOrEmpty_JobTrackerPortOnlyNotNull() throws Exception {
    when( config.getNamedCluster().getHdfsHost() ).thenReturn( null );
    when( config.getNamedCluster().getHdfsPort() ).thenReturn( null );
    when( config.getNamedCluster().getJobTrackerHost() ).thenReturn( null );
    boolean ncPropertiesNotNullOrEmpty = config.ncPropertiesNotNullOrEmpty( config.getNamedCluster() );
    assertTrue( "It should be true - Job tracker host: " + config.getNamedCluster().getJobTrackerPort(), ncPropertiesNotNullOrEmpty );
  }

  @Test
  public void testSetCustomArguments_GetCustomArguments() throws Exception {
    PropertyEntry pEntryMock = new PropertyEntry( ENTRY_KEY, ENTRY_VALUE );
    AbstractModelList<PropertyEntry> customArguments = new AbstractModelList<>();
    customArguments.add( pEntryMock );
    assertNotNull( config.getCustomArguments() );
    assertEquals( 0, config.getCustomArguments().size() );
    config.setCustomArguments( customArguments );
    assertSame( customArguments, config.getCustomArguments() );
    assertEquals( 1, config.getCustomArguments().size() );
    assertEquals( ENTRY_KEY, config.getCustomArguments().get( 0 ).getKey() );
    assertEquals( ENTRY_VALUE, config.getCustomArguments().get( 0 ).getValue() );
  }

}
