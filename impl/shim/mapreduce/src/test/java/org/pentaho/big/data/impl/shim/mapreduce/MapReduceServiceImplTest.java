/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.mapreduce;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.mapreduce.MapReduceExecutionException;
import org.pentaho.bigdata.api.mapreduce.MapReduceJarInfo;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobBuilder;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobSimple;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entries.hadoopjobexecutor.JarUtility;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 12/8/15.
 */
public class MapReduceServiceImplTest {
  private NamedCluster namedCluster;
  private HadoopShim hadoopShim;
  private ExecutorService executorService;
  private JarUtility jarUtility;
  private MapReduceServiceImpl mapReduceService;
  private URL resolvedJarUrl;

  @Before
  public void setup() throws MalformedURLException {
    namedCluster = mock( NamedCluster.class );
    hadoopShim = mock( HadoopShim.class );
    executorService = mock( ExecutorService.class );
    jarUtility = mock( JarUtility.class );
    resolvedJarUrl = new URL( "http://jar.net/jar" );
    mapReduceService = new MapReduceServiceImpl( namedCluster, hadoopShim, executorService, jarUtility );
  }

  @Test( expected = MapReduceExecutionException.class )
  public void testLocateDriverClassEmptyNoMains() throws MalformedURLException, MapReduceExecutionException {
    when(
      jarUtility.getClassesInJarWithMain( resolvedJarUrl.toExternalForm(), hadoopShim.getClass().getClassLoader() ) )
      .thenReturn(
        Collections.<Class<?>>emptyList() );
    try {
      mapReduceService.locateDriverClass( null, resolvedJarUrl, hadoopShim );
    } catch ( MapReduceExecutionException e ) {
      assertEquals(
        BaseMessages.getString( MapReduceServiceImpl.PKG, MapReduceServiceImpl.ERROR_DRIVER_CLASS_NOT_SPECIFIED ),
        e.getMessage() );
      throw e;
    }
  }

  @Test( expected = MapReduceExecutionException.class )
  public void testLocateDriverClassEmpty2Mains() throws MalformedURLException, MapReduceExecutionException {
    when(
      jarUtility.getClassesInJarWithMain( resolvedJarUrl.toExternalForm(), hadoopShim.getClass().getClassLoader() ) )
      .thenReturn( Arrays.asList( Object.class, String.class ) );
    try {
      mapReduceService.locateDriverClass( null, resolvedJarUrl, hadoopShim );
    } catch ( MapReduceExecutionException e ) {
      assertEquals(
        BaseMessages.getString( MapReduceServiceImpl.PKG, MapReduceServiceImpl.ERROR_MULTIPLE_DRIVER_CLASSES ),
        e.getMessage() );
      throw e;
    }
  }

  @Test
  public void testLocateDriverClassEmptySuccess() throws MalformedURLException, MapReduceExecutionException {
    when(
      jarUtility.getClassesInJarWithMain( resolvedJarUrl.toExternalForm(), hadoopShim.getClass().getClassLoader() ) )
      .thenReturn( Collections.<Class<?>>singletonList( String.class ) );
    assertEquals( String.class, mapReduceService.locateDriverClass( null, resolvedJarUrl, hadoopShim ) );
  }


  @Test
  public void testLocateDriverClassEmptyManifestSuccess()
    throws IOException, MapReduceExecutionException, ClassNotFoundException {
    when( jarUtility.getMainClassFromManifest( resolvedJarUrl, hadoopShim.getClass().getClassLoader() ) )
      .thenReturn( (Class) String.class );
    assertEquals( String.class, mapReduceService.locateDriverClass( null, resolvedJarUrl, hadoopShim ) );
  }

  @Test
  public void testLocateDriverClass() throws MapReduceExecutionException, IOException, ClassNotFoundException {
    String name = "name";
    when( jarUtility.getClassByName( name, resolvedJarUrl, hadoopShim.getClass().getClassLoader() ) )
      .thenReturn( (Class) String.class );
    assertEquals( String.class, mapReduceService.locateDriverClass( name, resolvedJarUrl, hadoopShim ) );
  }

  @Test( expected = MapReduceExecutionException.class )
  public void testLocateDriverClassRuntimeException()
    throws IOException, ClassNotFoundException, MapReduceExecutionException {
    String name = "name";
    RuntimeException runtimeException = new RuntimeException();
    when( jarUtility.getClassByName( name, resolvedJarUrl, hadoopShim.getClass().getClassLoader() ) )
      .thenThrow( runtimeException );
    try {
      mapReduceService.locateDriverClass( name, resolvedJarUrl, hadoopShim );
    } catch ( MapReduceExecutionException e ) {
      assertEquals( runtimeException, e.getCause() );
      throw e;
    }
  }

  @Test
  public void testExecuteSimple() throws MapReduceExecutionException, IOException, ClassNotFoundException {
    String name = "name";
    when( jarUtility.getClassByName( name, resolvedJarUrl, hadoopShim.getClass().getClassLoader() ) )
      .thenReturn( (Class) String.class );
    MapReduceJobSimple mapReduceJobSimple = mapReduceService.executeSimple( resolvedJarUrl, name, "cli" );
    assertNotNull( mapReduceJobSimple );
    assertEquals( String.class.getCanonicalName(), mapReduceJobSimple.getMainClass() );
  }

  @Test
  public void testCreateJobBuilder() {
    MapReduceJobBuilder jobBuilder =
      mapReduceService.createJobBuilder( mock( LogChannelInterface.class ), mock( VariableSpace.class ) );
    assertNotNull( jobBuilder );
  }

  @Test
  public void testGetJarInfoNoMain() throws IOException, ClassNotFoundException {
    when(
      jarUtility.getClassesInJarWithMain( resolvedJarUrl.toExternalForm(), hadoopShim.getClass().getClassLoader() ) )
      .thenReturn( Arrays.asList( Object.class, String.class ) );
    MapReduceJarInfo jarInfo = mapReduceService.getJarInfo( resolvedJarUrl );
    assertNull( jarInfo.getMainClass() );
    assertEquals( new ArrayList<>(
        Arrays.asList( Object.class.getCanonicalName(), String.class.getCanonicalName() ) ),
      jarInfo.getClassesWithMain() );
  }

  @Test
  public void testGetJarInfoMain() throws IOException, ClassNotFoundException {
    when(
      jarUtility.getMainClassFromManifest( resolvedJarUrl, hadoopShim.getClass().getClassLoader() ) )
      .thenReturn( (Class) String.class );
    MapReduceJarInfo jarInfo = mapReduceService.getJarInfo( resolvedJarUrl );
    assertEquals( new ArrayList<>(), jarInfo.getClassesWithMain() );
    assertEquals( String.class.getCanonicalName(), jarInfo.getMainClass() );
  }
}
