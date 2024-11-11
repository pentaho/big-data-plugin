/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */

@RunWith( MockitoJUnitRunner.class )
public class S3ClientImplTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private AmazonS3Client awsS3Client;
  private S3ClientImpl s3Client;
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

    s3Client = spy( new S3ClientImpl( awsS3Client ) );
  }

  @Test
  public void testReadStepLogsFromS3_whenLogFileExistsInS3() throws Exception {
    FileInputStream inputStream = null;
    S3ObjectInputStream s3ObjectInputStream = null;
    String stagingBucketName = "alzhk";
    String hadoopJobFlowId = "j-11WRZQW6NIQOA";
    String stepId = "s-15PK2NMVIPRPF";

    doReturn( "" )
      .when( s3Client ).readLogFromS3( stagingBucketName, hadoopJobFlowId + "/steps/" + stepId + "/controller.gz" );
    doReturn( "" )
      .when( s3Client ).readLogFromS3(  stagingBucketName, hadoopJobFlowId + "/steps/" + stepId + "/stdout.gz" );
    doReturn( "" )
      .when( s3Client ).readLogFromS3(  stagingBucketName, hadoopJobFlowId + "/steps/" + stepId + "/syslog.gz" );

    try {
      inputStream = new FileInputStream( gzArchName );
      s3ObjectInputStream = new S3ObjectInputStream( inputStream, null, false );
      S3Object s3Object = new S3Object();
      s3Object.setObjectContent( s3ObjectInputStream );

      Mockito.when( awsS3Client.getObject( anyString(), anyString() ) ).thenReturn( s3Object );
      Mockito.when( awsS3Client.doesObjectExist( anyString(), anyString() ) ).thenReturn( true );

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

    doReturn( "" )
      .when( s3Client ).readLogFromS3( eq( stagingBucketName ), anyString() );
    doCallRealMethod().when( s3Client ).readStepLogsFromS3( anyString(), anyString(), anyString() );

    try {
      inputStream = new FileInputStream( gzArchName );
      s3ObjectInputStream = new S3ObjectInputStream( inputStream, null, false );
      S3Object s3Object = new S3Object();
      s3Object.setObjectContent( s3ObjectInputStream );

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
