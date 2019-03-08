/*******************************************************************************
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
package org.pentaho.amazon.s3;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.trans.steps.mock.StepMockHelper;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputData;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.never;

public class S3FileOutputTest {

  private S3FileOutput s3FileOutput;
  private StepMockHelper<S3FileOutputMeta, TextFileOutputData> stepMockHelper;
  private S3FileOutputMeta smi;

  @BeforeClass
  public static void setClassUp() throws Exception {
    KettleEnvironment.init();
  }

  @AfterClass
  public static void tearDownClass() {
    KettleEnvironment.shutdown();
  }

  @Before
  public void setUp() {
    smi = mock( S3FileOutputMeta.class );
    stepMockHelper =
      new StepMockHelper<>( "S3 TEXT FILE OUTPUT TEST", S3FileOutputMeta.class, TextFileOutputData.class );
    when( stepMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      stepMockHelper.logChannelInterface );
    verify( stepMockHelper.logChannelInterface, never() ).logError( anyString() );
    verify( stepMockHelper.logChannelInterface, never() ).logError( anyString(), any( Object[].class ) );
    verify( stepMockHelper.logChannelInterface, never() ).logError( anyString(), (Throwable) anyObject() );
    when( stepMockHelper.trans.isRunning() ).thenReturn( true );
    verify( stepMockHelper.trans, never() ).stopAll();

    s3FileOutput = new S3FileOutput( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta,
      stepMockHelper.trans );

    System.setProperty( "aws.accessKeyId", "" );
    System.setProperty( "aws.secretKey", "" );
  }

  @After
  public void tearDown() {
    stepMockHelper.cleanUp();
    System.setProperty( "aws.accessKeyId", "" );
    System.setProperty( "aws.secretKey", "" );
  }

  @Test
  public void initWithDefaultCredentialsTest() {
    System.setProperty( Const.KETTLE_USE_AWS_DEFAULT_CREDENTIALS, "Y" );
    s3FileOutput.init( smi );

    Assert.assertEquals( "", System.getProperty( "aws.accessKeyId" ) );
    Assert.assertEquals( "", System.getProperty( "aws.secretKey" ) );
  }

  @Test
  public void initWithNoCredentialsTest() {
    System.setProperty( Const.KETTLE_USE_AWS_DEFAULT_CREDENTIALS, "N" );
    when( smi.getAccessKey() ).thenReturn( "" );
    when( smi.getSecretKey() ).thenReturn( "" );

    s3FileOutput.init( smi );

    Assert.assertEquals( "", System.getProperty( "aws.accessKeyId" ) );
    Assert.assertEquals( "", System.getProperty( "aws.secretKey" ) );
  }

  @Test
  public void initWithCredentialsTest() {
    System.setProperty( Const.KETTLE_USE_AWS_DEFAULT_CREDENTIALS, "N" );
    when( smi.getAccessKey() ).thenReturn( "accessKey" );
    when( smi.getSecretKey() ).thenReturn( "secretKey" );

    s3FileOutput.init( smi );

    Assert.assertEquals( "accessKey", System.getProperty( "aws.accessKeyId" ) );
    Assert.assertEquals( "secretKey", System.getProperty( "aws.secretKey" ) );
  }
}
