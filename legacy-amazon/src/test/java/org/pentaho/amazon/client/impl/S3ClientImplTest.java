/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon.client.impl;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.pentaho.amazon.client.api.S3Client;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;


/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */

@RunWith( PowerMockRunner.class )
@PrepareForTest( S3ClientImpl.class )
@PowerMockIgnore( { "javax.management.*", "jdk.internal.reflect.*" } )
public class S3ClientImplTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private AmazonS3Client awsS3Client;
  private S3Client s3Client;
  private String logFileName;
  private String gzArchName;
  private File stagingFolder;

  @Before
  public void setUp() throws Exception {
    stagingFolder = temporaryFolder.newFolder( "emr" );
    String path = getClass().getClassLoader().getResource( "master.log" ).getPath();
    File logFile = new File( path );
    FileUtils.copyFileToDirectory( logFile, stagingFolder );

    logFileName = Paths.get( stagingFolder.getPath(), "master.log" ).toString();
    gzArchName = Paths.get( stagingFolder.getPath(), "stderr.gz" ).toString();

    createGzArchive();

    s3Client = PowerMockito.spy( new S3ClientImpl( awsS3Client ) );
  }

  @Test
  public void testReadStepLogsFromS3_whenLogFileExistsInS3() throws Exception {
    FileInputStream inputStream = null;
    S3ObjectInputStream s3ObjectInputStream = null;
    String stagingBucketName = "alzhk";
    String hadoopJobFlowId = "j-11WRZQW6NIQOA";
    String stepId = "s-15PK2NMVIPRPF";

    PowerMockito.doReturn( "" )
      .when( s3Client, "readLogFromS3", stagingBucketName, hadoopJobFlowId + "/steps/" + stepId + "/controller.gz" );
    PowerMockito.doReturn( "" )
      .when( s3Client, "readLogFromS3", stagingBucketName, hadoopJobFlowId + "/steps/" + stepId + "/stdout.gz" );
    PowerMockito.doReturn( "" )
      .when( s3Client, "readLogFromS3", stagingBucketName, hadoopJobFlowId + "/steps/" + stepId + "/syslog.gz" );

    try {
      inputStream = new FileInputStream( gzArchName );
      s3ObjectInputStream = new S3ObjectInputStream( inputStream, null, false );
      S3Object s3Object = new S3Object();
      s3Object.setObjectContent( s3ObjectInputStream );

      Mockito.when( awsS3Client.getObject( Matchers.anyString(), Matchers.anyString() ) ).thenReturn( s3Object );
      Mockito.when( awsS3Client.doesObjectExist( Matchers.anyString(), Matchers.anyString() ) ).thenReturn( true );

      String lineFromStepLogFile = s3Client.readStepLogsFromS3( stagingBucketName, hadoopJobFlowId, stepId );
      lineFromStepLogFile = lineFromStepLogFile.replace( "\n", "" ).replace( "\r", "" );
      Assert.assertEquals( "all bootstrap actions complete and instance ready", lineFromStepLogFile );
    } finally {
      if ( inputStream != null ) {
        inputStream.close();
      }
      if ( s3ObjectInputStream != null ) {
        s3ObjectInputStream.close();
      }
    }
  }

  @Test
  public void testReadStepLogsFromS3_whenLogFileNotExistsInS3() throws Exception {
    FileInputStream inputStream = null;
    S3ObjectInputStream s3ObjectInputStream = null;
    String stagingBucketName = "alzhk";
    String hadoopJobFlowId = "j-11WRZQW6NIQOA";
    String stepId = "s-15PK2NMVIPRPF";
    String expectedLineFromStepLogFile =
      "Step " + stepId + " failed. See logs here: s3://" + stagingBucketName + "/" + hadoopJobFlowId + "/steps/"
        + stepId;

    PowerMockito.doReturn( "" )
      .when( s3Client, "readLogFromS3", Matchers.eq( stagingBucketName ), Matchers.anyString() );

    try {
      inputStream = new FileInputStream( gzArchName );
      s3ObjectInputStream = new S3ObjectInputStream( inputStream, null, false );
      S3Object s3Object = new S3Object();
      s3Object.setObjectContent( s3ObjectInputStream );

      Mockito.when( awsS3Client.doesObjectExist( Matchers.anyString(), Matchers.anyString() ) ).thenReturn( false );

      String lineFromStepLogFile = s3Client.readStepLogsFromS3( stagingBucketName, hadoopJobFlowId, stepId );
      lineFromStepLogFile = lineFromStepLogFile.replace( "\n", "" ).replace( "\r", "" );

      Assert.assertEquals( expectedLineFromStepLogFile, lineFromStepLogFile );
    } finally {
      if ( inputStream != null ) {
        inputStream.close();
      }
      if ( s3ObjectInputStream != null ) {
        s3ObjectInputStream.close();
      }
    }
  }

  private void createGzArchive() throws Exception {

    try ( FileInputStream fileInputStream = new FileInputStream( logFileName );
         FileOutputStream fileOutputStream = new FileOutputStream( gzArchName );
         GZIPOutputStream gzipOutputStream = new GZIPOutputStream( fileOutputStream ) ) {
      byte[] buffer = new byte[ 1024 ];
      int len;
      while ( ( len = fileInputStream.read( buffer ) ) != -1 ) {
        gzipOutputStream.write( buffer, 0, len );
      }
    }
  }
}
