/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.sqoop;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/26/16.
 */
public class SqoopServiceImplTest {
  private HadoopShim hadoopShim;
  private SqoopShim sqoopShim;
  private NamedCluster namedCluster;
  private SqoopServiceImpl sqoopService;
  private Configuration configuration;

  @Before
  public void setup() {
    hadoopShim = mock( HadoopShim.class );
    sqoopShim = mock( SqoopShim.class );
    namedCluster = mock( NamedCluster.class );
    sqoopService = new SqoopServiceImpl( hadoopShim, sqoopShim, namedCluster );
    configuration = mock( Configuration.class );
    when( hadoopShim.createConfiguration() ).thenReturn( configuration );
  }

  @Test
  public void testRunToolSuccess() throws Exception {
    Properties properties = new Properties();
    String testKey = "testKey";
    String testValue = "testValue";
    properties.setProperty( testKey, testValue );
    when( namedCluster.isMapr() ).thenReturn( true );

    String[] testArgs = { "testARgs" };
    assertEquals( 0, sqoopService.runTool( new ArrayList<>( Arrays.asList( testArgs ) ), properties ) );

    verify( configuration ).set( testKey, testValue );
    verify( hadoopShim )
      .configureConnectionInformation( eq( "" ), eq( "" ), eq( "" ), eq( "" ), eq( configuration ), any( List.class ) );
    verify( sqoopShim ).runTool( eq( testArgs ), eq( configuration ) );
    assertEquals( Boolean.toString( true ), System.getProperty( SqoopServiceImpl.SQOOP_THROW_ON_ERROR ) );
  }

  @Test
  public void testRunToolFailure() {
    when( namedCluster.isMapr() ).thenThrow( new RuntimeException() );
    assertEquals( 1, sqoopService.runTool( new ArrayList<String>(), new Properties() ) );
  }

  @Test
  public void testConfigureShimNotMapR() throws Exception {
    String[] vars = new String[] { "var1", "var2", "var3", "var4" };
    String[] vals = new String[] { "hdfsHost", "hdfsPort", "jobTrackerHost", "jobTrackerPort" };

    int i = 0;
    when( namedCluster.getHdfsHost() ).thenReturn( vars[ i++ ] );
    when( namedCluster.getHdfsPort() ).thenReturn( vars[ i++ ] );
    when( namedCluster.getJobTrackerHost() ).thenReturn( vars[ i++ ] );
    when( namedCluster.getJobTrackerPort() ).thenReturn( vars[ i++ ] );

    for ( i = 0; i < vars.length; i++ ) {
      when( namedCluster.environmentSubstitute( vars[ i ] ) ).thenReturn( vals[ i ] );
    }

    i = 0;
    final String testVal = "testVal";
    doAnswer( new Answer<Void>() {
      @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
        ( (List<String>) invocation.getArguments()[ 5 ] ).add( testVal );
        return null;
      }
    } ).when( hadoopShim )
      .configureConnectionInformation( eq( vals[ i++ ] ), eq( vals[ i++ ] ), eq( vals[ i++ ] ), eq( vals[ i++ ] ),
        eq( configuration ), eq( new ArrayList<String>() ) );

    sqoopService.configureShim( configuration );
    i = 0;
    verify( hadoopShim )
      .configureConnectionInformation( eq( vals[ i++ ] ), eq( vals[ i++ ] ), eq( vals[ i++ ] ), eq( vals[ i++ ] ),
        eq( configuration ), eq( new ArrayList<>( Arrays.asList( testVal ) ) ) );
  }
}
