/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.pentaho.amazon.client.api.S3Client;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public class S3ClientImpl implements S3Client {

  private AmazonS3 s3Client;

  public S3ClientImpl( AmazonS3 s3Client ) {
    this.s3Client = s3Client;
  }

  @Override
  public void createBucketIfNotExists( String stagingBucketName ) {
    if ( !s3Client.doesBucketExistV2( stagingBucketName ) ) {
      s3Client.createBucket( stagingBucketName );
    }
  }

  @Override
  public void deleteObjectFromBucket( String stagingBucketName, String key ) {
    s3Client.deleteObject( stagingBucketName, key );
  }

  @Override
  public void putObjectInBucket( String stagingBucketName, String key, File tmpFile ) {
    s3Client.putObject( new PutObjectRequest( stagingBucketName, key, tmpFile ) );
  }

  @Override
  public String readStepLogsFromS3( String stagingBucketName, String hadoopJobFlowId, String stepId ) {

    String lineSeparator = System.getProperty( "line.separator" );
    String[] logArchives = { "/controller.gz", "/stdout.gz", "/syslog.gz", "/stderr.gz" };
    StringBuilder logContents = new StringBuilder();
    String logFromS3File = "";
    String pathToStepLogs = "";

    for ( String gzLogFile : logArchives ) {
      logFromS3File = readLogFromS3( stagingBucketName, hadoopJobFlowId + "/steps/" + stepId + gzLogFile );
      if ( logFromS3File != null && !logFromS3File.isEmpty() ) {
        logContents.append( logFromS3File + lineSeparator );
      }
    }
    if ( logContents.length() == 0 ) {
      pathToStepLogs = "s3://" + stagingBucketName + "/" + hadoopJobFlowId + "/steps/" + stepId;
      logContents.append( "Step " + stepId + " failed. See logs here: " + pathToStepLogs + lineSeparator );
    }
    return logContents.toString();
  }

  protected String readLogFromS3( String stagingBucketName, String key ) {

    Scanner logScanner = null;
    S3ObjectInputStream s3ObjectInputStream = null;
    GZIPInputStream gzipInputStream = null;
    String lineSeparator = System.getProperty( "line.separator" );
    StringBuilder logContents = new StringBuilder();
    S3Object outObject;

    try {
      if ( s3Client.doesObjectExist( stagingBucketName, key ) ) {

        outObject = s3Client.getObject( stagingBucketName, key );
        s3ObjectInputStream = outObject.getObjectContent();
        gzipInputStream = new GZIPInputStream( s3ObjectInputStream );

        logScanner = new Scanner( gzipInputStream );
        while ( logScanner.hasNextLine() ) {
          logContents.append( logScanner.nextLine() + lineSeparator );
        }
      }
    } catch ( IOException e ) {
      e.printStackTrace();
    } finally {
      try {
        if ( logScanner != null ) {
          logScanner.close();
        }
        if ( s3ObjectInputStream != null ) {
          s3ObjectInputStream.close();
        }
        if ( gzipInputStream != null ) {
          gzipInputStream.close();
        }
      } catch ( IOException e ) {
        //do nothing
      }
    }
    return logContents.toString();
  }
}
