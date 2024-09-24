/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.plugins.common.ui;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogChannelInterfaceFactory;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.runtime.test.RuntimeTest;
import org.pentaho.runtime.test.RuntimeTestStatus;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.module.RuntimeTestModuleResults;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResult;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;

import java.util.ArrayList;

public class TestClusterTestDialog {

  private static LogChannelInterfaceFactory oldLogChannelInterfaceFactory;
  private static LogChannelInterface logChannelInterface;

  private ClusterTestDialog testDialog;
  private Shell parent;
  private NamedCluster namedCluster;
  private RuntimeTester runtimeTester;
  private PropsUI props;

  @BeforeClass
  public static void beforeClass() {
    KettleLogStore.init();
    oldLogChannelInterfaceFactory = KettleLogStore.getLogChannelInterfaceFactory();
    setKettleLogFactoryWithMock();
  }

  public static void setKettleLogFactoryWithMock() {
    LogChannelInterfaceFactory logChannelInterfaceFactory = Mockito.mock( LogChannelInterfaceFactory.class );
    logChannelInterface = Mockito.mock( LogChannelInterface.class );
    Mockito.when( logChannelInterfaceFactory.create( Mockito.any() ) ).thenReturn( logChannelInterface );
    KettleLogStore.setLogChannelInterfaceFactory( logChannelInterfaceFactory );
  }

  @Before
  public void setup() throws KettleException {
    parent = Mockito.mock( Shell.class );
    namedCluster = Mockito.mock( NamedCluster.class );
    runtimeTester = Mockito.mock( RuntimeTester.class );
    props = Mockito.mock( PropsUI.class );

    testDialog = new ClusterTestDialog( parent, namedCluster, runtimeTester ) {

      @Override
      protected PropsUI getPropsUIInstance() {
        return props;
      }

      @Override
      public void dispose() {
      }
    };
  }

  @Test
  public void testExceptionIsPrintedToLog() throws KettleException {
    ProgressBar progressBar = Mockito.mock( ProgressBar.class );
    RuntimeTestStatus clusterTestStatus = Mockito.mock( RuntimeTestStatus.class );
    Label testLabel = Mockito.mock( Label.class );
    RuntimeTestModuleResults runtimeTestModuleResults = Mockito.mock( RuntimeTestModuleResults.class );
    RuntimeTestResult result = Mockito.mock( RuntimeTestResult.class );
    RuntimeTestResultEntry entry = Mockito.mock( RuntimeTestResultEntry.class );
    Exception exception = new Exception();

    ArrayList<RuntimeTestModuleResults> results = new ArrayList<>();
    results.add( runtimeTestModuleResults );
    ArrayList<RuntimeTestResult> runtimeTestResults = new ArrayList<>();
    runtimeTestResults.add( result );

    Mockito.when( clusterTestStatus.getModuleResults() ).thenReturn( results );
    Mockito.when( clusterTestStatus.isDone() ).thenReturn( true );
    Mockito.when( runtimeTestModuleResults.getRuntimeTestResults() ).thenReturn( runtimeTestResults );
    Mockito.when( result.getRuntimeTest() ).thenReturn( Mockito.mock( RuntimeTest.class ) );
    Mockito.when( result.getRuntimeTestResultEntries() ).thenReturn( new ArrayList<>() );
    Mockito.when( result.getOverallStatusEntry() ).thenReturn( entry );
    Mockito.when( entry.getSeverity() ).thenReturn( RuntimeTestEntrySeverity.FATAL );
    Mockito.when( entry.getException() ).thenReturn( exception );

    testDialog.getRunnable( progressBar, clusterTestStatus, testLabel ).run();

    Mockito.verify( logChannelInterface,  Mockito.times( 1 ) ).logBasic( ExceptionUtils.getStackTrace( exception ) );
  }

  @Test
  public void testExceptionsArePrintedToLog() throws KettleException {
    ProgressBar progressBar = Mockito.mock( ProgressBar.class );
    RuntimeTestStatus clusterTestStatus = Mockito.mock( RuntimeTestStatus.class );
    Label testLabel = Mockito.mock( Label.class );
    RuntimeTestModuleResults runtimeTestModuleResults = Mockito.mock( RuntimeTestModuleResults.class );
    RuntimeTestResult result = Mockito.mock( RuntimeTestResult.class );
    RuntimeTestResultEntry entry = Mockito.mock( RuntimeTestResultEntry.class );
    Exception exception = new Exception();

    ArrayList<RuntimeTestModuleResults> results = new ArrayList<>();
    results.add( runtimeTestModuleResults );
    ArrayList<RuntimeTestResult> runtimeTestResults = new ArrayList<>();
    runtimeTestResults.add( result );
    ArrayList<RuntimeTestResultEntry> entries = new ArrayList<>();
    entries.add( entry );
    entries.add( entry );

    Mockito.when( clusterTestStatus.getModuleResults() ).thenReturn( results );
    Mockito.when( clusterTestStatus.isDone() ).thenReturn( true );
    Mockito.when( runtimeTestModuleResults.getRuntimeTestResults() ).thenReturn( runtimeTestResults );
    Mockito.when( result.getRuntimeTest() ).thenReturn( Mockito.mock( RuntimeTest.class ) );
    Mockito.when( result.getRuntimeTestResultEntries() ).thenReturn( entries );
    Mockito.when( entry.getSeverity() ).thenReturn( RuntimeTestEntrySeverity.FATAL );
    Mockito.when( entry.getException() ).thenReturn( exception );

    testDialog.getRunnable( progressBar, clusterTestStatus, testLabel ).run();

    Mockito.verify( logChannelInterface,  Mockito.times( 2 ) ).logBasic( ExceptionUtils.getStackTrace( exception ) );
  }

  @AfterClass
  public static void tearDown() {
    KettleLogStore.setLogChannelInterfaceFactory( oldLogChannelInterfaceFactory );
  }
}
