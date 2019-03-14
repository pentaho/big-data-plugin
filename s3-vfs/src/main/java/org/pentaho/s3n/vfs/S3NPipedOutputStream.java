/*!
 * Copyright 2010 - 2019 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.s3n.vfs;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.apache.commons.vfs2.FileSystemOptions;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Custom OutputStream that enables chunked uploads into S3
 */
public class S3NPipedOutputStream extends PipedOutputStream {
  private static int PART_SIZE = 1024 * 1024 * 5;
  private ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool( 1 );
  private boolean initialized = false;
  private boolean blockedUntilDone = true;
  private PipedInputStream pipedInputStream;
  private S3AsyncTransferRunner s3AsyncTransferRunner;
  private S3NFileSystem fileSystem;
  private Future<Boolean> result = null;
  private String bucketId;
  private String key;

  public S3NPipedOutputStream( S3NFileSystem fileSystem, String bucketId, String key ) throws IOException {
    this.pipedInputStream = new PipedInputStream();

    try {
      this.pipedInputStream.connect( this );
    } catch ( IOException e ) {
      // FATAL, unexpected
      throw new RuntimeException( e );
    }

    this.s3AsyncTransferRunner = new S3AsyncTransferRunner();
    this.bucketId = bucketId;
    this.key = key;
    this.fileSystem = fileSystem;
  }

  private void initializeWrite() {
    if ( !initialized ) {
      initialized = true;
      result = this.executor.submit( s3AsyncTransferRunner );
    }
  }

  public boolean isBlockedUntilDone() {
    return blockedUntilDone;
  }

  public void setBlockedUntilDone( boolean blockedUntilDone ) {
    this.blockedUntilDone = blockedUntilDone;
  }

  @Override
  public void write( int b ) throws IOException {
    initializeWrite();
    super.write( b );
  }

  @Override
  public void write( byte[] b, int off, int len ) throws IOException {
    initializeWrite();
    super.write( b, off, len );
  }

  @Override
  public void close() throws IOException {
    super.close();

    if ( initialized ) {
      if ( isBlockedUntilDone() ) {
        while ( !result.isDone() ) {
          try {
            Thread.sleep( 100 );
          } catch ( InterruptedException e ) {
            e.printStackTrace();
          }
        }
      }
    }

    this.executor.shutdown();
  }

  class S3AsyncTransferRunner implements Callable<Boolean> {

    public Boolean call() throws Exception {
      boolean result = true;
      List<PartETag> partETags = new ArrayList<PartETag>();

      // Step 1: Initialize
      FileSystemOptions fsOpts = fileSystem.getFileSystemOptions();

      InitiateMultipartUploadRequest initRequest;
      ObjectMetadata objectMetadata;
      initRequest = new InitiateMultipartUploadRequest( bucketId, key );

      InitiateMultipartUploadResult initResponse = null;
      ByteArrayOutputStream baos = new ByteArrayOutputStream( PART_SIZE );

      try {
        initResponse = fileSystem.getS3Client().initiateMultipartUpload( initRequest );
        // Step 2: Upload parts.
        byte[] tmpBuffer = new byte[ PART_SIZE ];
        int read, offset = 0;
        int totalRead = 0;
        int partNum = 1;
        BufferedInputStream bis = new BufferedInputStream( pipedInputStream, PART_SIZE );
        S3NWindowedSubstream s3is;

        while ( ( read = bis.read( tmpBuffer ) ) >= 0 ) {

          // if something was actually read
          if ( read > 0 ) {
            baos.write( tmpBuffer, 0, read );
            totalRead += read;
          }

          if ( totalRead > PART_SIZE ) { // do we have a minimally accepted chunk above 5Mb?
            s3is = new S3NWindowedSubstream( baos.toByteArray() );

            UploadPartRequest uploadRequest = new UploadPartRequest()
              .withBucketName( bucketId ).withKey( key )
              .withUploadId( initResponse.getUploadId() ).withPartNumber( partNum++ )
              .withFileOffset( offset )
              .withPartSize( totalRead )
              .withInputStream( s3is );

            // Upload part and add response to our list.
            partETags.add( fileSystem.getS3Client().uploadPart( uploadRequest ).getPartETag() );

            offset += totalRead;
            totalRead = 0; // reset part size counter
            baos.reset(); // reset output stream to 0
          }
        }

        // Step 2.1 upload last part
        s3is = new S3NWindowedSubstream( baos.toByteArray() );

        UploadPartRequest uploadRequest = new UploadPartRequest()
          .withBucketName( bucketId ).withKey( key )
          .withUploadId( initResponse.getUploadId() ).withPartNumber( partNum++ )
          .withFileOffset( offset )
          .withPartSize( totalRead )
          .withInputStream( s3is )
          .withLastPart( true );

        partETags.add( fileSystem.getS3Client().uploadPart( uploadRequest ).getPartETag() );

        // Step 3: Complete.
        CompleteMultipartUploadRequest compRequest =
          new CompleteMultipartUploadRequest( bucketId, key, initResponse.getUploadId(), partETags );

        fileSystem.getS3Client().completeMultipartUpload( compRequest );
      } catch ( Exception e ) {
        e.printStackTrace();
        if ( initResponse == null ) {
          close();
        } else {
          fileSystem.getS3Client()
            .abortMultipartUpload( new AbortMultipartUploadRequest( bucketId, key, initResponse.getUploadId() ) );
        }
        result = false;
      } finally {
        baos.close();
      }

      return result;
    }
  }
}
