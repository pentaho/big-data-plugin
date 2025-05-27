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


package org.pentaho.s3common;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StorageUnitConverter;
import org.pentaho.di.i18n.BaseMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OutputStream for high-throughput, streaming multipart uploads to S3 using a producer-consumer pattern.
 * <p>
 * This stream buffers data into S3-compliant part sizes and uploads them in parallel using a thread pool.
 * It is robust against deadlocks, enforces S3 part size/count limits, and provides detailed logging for debugging.
 * <p>
 * Usage:
 * <ul>
 *   <li>Write data to this stream as with any OutputStream.</li>
 *   <li>On close(), all parts are uploaded and the multipart upload is finalized.</li>
 * </ul>
 *
 * @author Pentaho
 * @since 2024
 */
public class S3CommonPipedOutputStream extends PipedOutputStream {

  private static final Class<?> PKG = S3CommonPipedOutputStream.class;
  private static final Logger logger = LoggerFactory.getLogger( S3CommonPipedOutputStream.class );
  private static final LogChannelInterface consoleLog = new LogChannel( BaseMessages.getString( PKG, "TITLE.S3File" ) );

  public static final int DEFAULT_PART_SIZE = 128 * 1024 * 1024; // Minimum part size is 5MB, but we use 128MB for better performance
  public static final int DEFAULT_THREAD_POOL_SIZE = 8;
  public static final byte[] POISON_PILL = new byte[0];

  private ExecutorService controllerExecutor; // single thread for S3AsyncTransferRunner
  private ExecutorService consumerExecutor;   // pool for consumers only
  private boolean initialized = false;
  private boolean blockedUntilDone = true;
  private PipedInputStream pipedInputStream;
  private S3AsyncTransferRunner s3AsyncTransferRunner;
  private S3CommonFileSystem fileSystem;
  private Future<Boolean> result = null;
  private String bucketId;
  private String key;
  private int partSize; // AWS Multipart part size.
  private int threadPoolSize;

  /**
   * Constructs a new S3CommonPipedOutputStream with default part size and thread pool size.
   *
   * @param fileSystem the S3 file system
   * @param bucketId   the S3 bucket
   * @param key        the S3 object key
   * @throws IOException if the stream cannot be initialized
   */
  public S3CommonPipedOutputStream( S3CommonFileSystem fileSystem, String bucketId, String key ) throws IOException {
    this( fileSystem, bucketId, key, DEFAULT_PART_SIZE, DEFAULT_THREAD_POOL_SIZE );
  }

  /**
   * Constructs a new S3CommonPipedOutputStream.
   *
   * @param fileSystem the S3 file system
   * @param bucketId   the S3 bucket
   * @param key        the S3 object key
   * @param partSize   the multipart part size (min 5MB)
   * @param threadPoolSize the number of parallel upload threads (min 1)
   * @throws IOException if the stream cannot be initialized
   * @throws IllegalArgumentException if partSize or threadPoolSize are invalid
   */
  public S3CommonPipedOutputStream( S3CommonFileSystem fileSystem, String bucketId, String key,
                                    int partSize, int threadPoolSize ) throws IOException {
    if ( partSize < 5 * 1024 * 1024 ) { // S3 minimum part size is 5MB
      throw new IllegalArgumentException( "partSize must be at least 5MB" );
    }
    if ( threadPoolSize < 1 ) {
      throw new IllegalArgumentException( "threadPoolSize must be at least 1" );
    }
    this.pipedInputStream = new PipedInputStream();
    try {
      this.pipedInputStream.connect( this );
    } catch ( IOException e ) {
      throw new IOException( "could not connect to pipedInputStream", e );
    }
    this.s3AsyncTransferRunner = new S3AsyncTransferRunner();
    this.bucketId = bucketId;
    this.key = key;
    this.fileSystem = fileSystem;
    this.partSize = partSize;
    this.threadPoolSize = threadPoolSize;
    this.controllerExecutor = Executors.newSingleThreadExecutor();
    this.consumerExecutor = Executors.newFixedThreadPool( threadPoolSize );
  }

  private void initializeWrite() {
    if ( !initialized ) {
      initialized = true;
      result = this.controllerExecutor.submit( s3AsyncTransferRunner );
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
    IOException thrown = null;
    try {
      super.close();
    } catch ( IOException e ) {
      logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.SuperClose" ), e );
      thrown = e;
    }
    if ( initialized && isBlockedUntilDone() ) {
      while ( !result.isDone() ) {
        try {
          Thread.sleep( 100 );
        } catch ( InterruptedException e ) {
          logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.InterruptedConsumerShutdown" ), e );
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    consumerExecutor.shutdown();
    controllerExecutor.shutdown();
    try {
      boolean consumerTerminated = consumerExecutor.awaitTermination( 60, java.util.concurrent.TimeUnit.SECONDS );
      if ( !consumerTerminated ) {
        logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.ConsumerNotTerminated" ) );
      }
    } catch ( InterruptedException e ) {
      logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.InterruptedConsumerShutdown" ), e );
      Thread.currentThread().interrupt();
    }
    try {
      boolean controllerTerminated = controllerExecutor.awaitTermination( 60, java.util.concurrent.TimeUnit.SECONDS );
      if ( !controllerTerminated ) {
        logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.ControllerNotTerminated" ) );
      }
    } catch ( InterruptedException e ) {
      logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.InterruptedControllerShutdown" ), e );
      Thread.currentThread().interrupt();
    }
    if ( thrown != null ) throw thrown;
  }

  class S3AsyncTransferRunner implements Callable<Boolean> {
    public Boolean call() throws Exception {
      boolean returnVal = true;
      int queueCapacity = Math.max( threadPoolSize * 2, 4 ); // 2x threads, min 4 for pipelining
      List<Future<List<PartETagWithNum>>> partFutures = new ArrayList<>();
      List<PartETagWithNum> partETagsWithNum = new ArrayList<>();
      InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest( bucketId, key );
      InitiateMultipartUploadResult initResponse = null;
      BlockingQueue<PartBuffer> queue = new LinkedBlockingQueue<>( queueCapacity );
      AtomicBoolean producerFailed = new AtomicBoolean( false );
      AtomicBoolean consumerFailed = new AtomicBoolean( false );
      Throwable[] consumerException = new Throwable[ 1 ];
      long startTime = System.currentTimeMillis();
      try {
        initResponse = fileSystem.getS3Client().initiateMultipartUpload( initRequest );
        final InitiateMultipartUploadResult finalInitResponse = initResponse;
        Thread producer = createProducer( queue, producerFailed );
        partFutures = startConsumers( queue, finalInitResponse, consumerFailed, consumerException );
        producer.start();
        producer.join();
        if ( producerFailed.get() ) throw new IOException( "Producer failed" );
        for ( Future<List<PartETagWithNum>> f : partFutures ) {
          try {
            partETagsWithNum.addAll( f.get() );
          } catch ( Exception ce ) {
            consumerFailed.set( true );
            consumerException[ 0 ] = ce;
            break;
          }
        }
        if ( consumerFailed.get() && consumerException[ 0 ] != null ) {
          throw new IOException( "Consumer failed", consumerException[ 0 ] );
        }
        partETagsWithNum.sort( ( a, b ) -> Integer.compare( a.partNum, b.partNum ) );
        List<PartETag> partETags = new ArrayList<>();
        for ( PartETagWithNum p : partETagsWithNum ) partETags.add( p.partETag );
        logger.info( BaseMessages.getString( PKG, "INFO.S3MultiPart.Complete" ) );
        logger.info( "Total upload time: {} ms", ( System.currentTimeMillis() - startTime ) );
        CompleteMultipartUploadRequest compRequest =
          new CompleteMultipartUploadRequest( bucketId, key, finalInitResponse.getUploadId(), partETags );
        fileSystem.getS3Client().completeMultipartUpload( compRequest );
      } catch ( OutOfMemoryError oome ) {
        consoleLog.logError( BaseMessages.getString( PKG,
          "ERROR.S3MultiPart.UploadOutOfMemory", new StorageUnitConverter().byteCountToDisplaySize( partSize ) ),
          oome );
        returnVal = false;
      } catch ( Exception e ) {
        logger.error( BaseMessages.getString( PKG, "ERROR.S3MultiPart.ExceptionCaught" ), e );
        try {
          if ( initResponse == null ) {
            close();
          } else {
            fileSystem.getS3Client()
              .abortMultipartUpload( new AbortMultipartUploadRequest( bucketId, key, initResponse.getUploadId() ) );
          }
        } catch ( Exception abortEx ) {
          logger.error( BaseMessages.getString( PKG, "ERROR.S3MultiPart.AbortOrClose" ), abortEx );
        }
        returnVal = false;
      }
      return returnVal;
    }

    private Thread createProducer( BlockingQueue<PartBuffer> queue, AtomicBoolean producerFailed ) {
      return new Thread( () -> {
        long producerStart = System.currentTimeMillis();
        try ( BufferedInputStream bis = new BufferedInputStream( pipedInputStream, partSize ) ) {
          byte[] partBuffer = new byte[ partSize ];
          int partBufferPos = 0;
          int partNum = 1;
          long offset = 0;
          byte[] tmpBuffer = new byte[ 64 * 1024 ]; // 64KB temp buffer for reads
          int read;
          while ( ( read = bis.read( tmpBuffer ) ) >= 0 ) {
            int srcPos = 0;
            while ( srcPos < read ) {
              int space = partSize - partBufferPos;
              int copyLen = Math.min( space, read - srcPos );
              System.arraycopy( tmpBuffer, srcPos, partBuffer, partBufferPos, copyLen );
              partBufferPos += copyLen;
              srcPos += copyLen;
              if ( partBufferPos == partSize ) {
                logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ProducerPuttingPart", partNum, offset, partSize ) );
                queue.put( new PartBuffer( partBuffer, partNum++, offset, partSize ) );
                offset += partSize;
                partBuffer = new byte[ partSize ];
                partBufferPos = 0;
              }
            }
          }
          // enqueue any remaining data as the final part
          if ( partBufferPos > 0 ) {
            byte[] lastPart = java.util.Arrays.copyOf( partBuffer, partBufferPos );
            logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ProducerPuttingFinalPart", partNum, offset, partBufferPos ) );
            queue.put( new PartBuffer( lastPart, partNum++, offset, partBufferPos ) );
          }
          // Insert POISON_PILLs using offer with timeout to avoid deadlock if queue is full
          for ( int i = 0; i < threadPoolSize; i++ ) {
            boolean inserted = false;
            while ( !inserted ) {
              inserted = queue.offer( new PartBuffer( POISON_PILL, -1, -1, -1 ), 5, java.util.concurrent.TimeUnit.SECONDS );
              if ( !inserted ) {
                logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.WaitingToInsertPoisonPill", i ) );
              }
            }
            logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.InsertedPoisonPill", i ) );
          }
          logger.info( BaseMessages.getString( PKG, "INFO.S3MultiPart.ProducerFinished", ( System.currentTimeMillis() - producerStart ) ) );
        } catch ( Exception e ) {
          producerFailed.set( true );
          // Try to insert POISON_PILLs even on failure
          for ( int i = 0; i < threadPoolSize; i++ ) {
            boolean inserted = false;
            while ( !inserted ) {
              try {
                inserted = queue.offer( new PartBuffer( POISON_PILL, -1, -1, -1 ), 5, java.util.concurrent.TimeUnit.SECONDS );
                if ( !inserted ) {
                  logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.WaitingToInsertPoisonPill", i ) );
                }
              } catch ( InterruptedException ie ) {
                logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.ProducerInterrupted" ), ie );
                Thread.currentThread().interrupt();
                break;
              }
            }
            logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ProducerInsertedPoisonPillOnError", i ) );
          }
          logger.error( BaseMessages.getString( PKG, "ERROR.S3MultiPart.ProducerFailed" ), e );
        }
      } );
    }

    private List<Future<List<PartETagWithNum>>> startConsumers(BlockingQueue<PartBuffer> queue, InitiateMultipartUploadResult finalInitResponse, AtomicBoolean consumerFailed, Throwable[] consumerException) {
      List<Future<List<PartETagWithNum>>> partFutures = new ArrayList<>();
      for ( int i = 0; i < threadPoolSize; i++ ) {
        final int consumerId = i;
        partFutures.add( consumerExecutor.submit( () -> {
          long consumerStart = System.currentTimeMillis();
          List<PartETagWithNum> localEtags = new ArrayList<>();
          try {
            while ( true ) {
              logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ConsumerWaitingForPart", consumerId ) );
              PartBuffer part = queue.take();
              if ( part.partBytes == POISON_PILL ) {
                logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ConsumerReceivedPoisonPill", consumerId ) );
                break;
              }
              logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ConsumerGotPart", consumerId, part.partNum, part.offset, part.length ) );
              S3CommonWindowedSubstream s3is = new S3CommonWindowedSubstream( part.partBytes );
              UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName( bucketId ).withKey( key )
                .withUploadId( finalInitResponse.getUploadId() ).withPartNumber( part.partNum )
                .withFileOffset( part.offset )
                .withPartSize( part.length )
                .withInputStream( s3is )
                .withLastPart( false );
              long uploadStart = System.currentTimeMillis();
              logger.info( BaseMessages.getString( PKG, "INFO.S3MultiPart.Upload", part.partNum, part.offset, part.length ));
              PartETag etag = fileSystem.getS3Client().uploadPart( uploadRequest ).getPartETag();
              logger.info( BaseMessages.getString( PKG, "INFO.S3MultiPart.PartUploaded", part.partNum, ( System.currentTimeMillis() - uploadStart ) ) );
              localEtags.add( new PartETagWithNum( etag, part.partNum ) );
            }
            logger.info( BaseMessages.getString( PKG, "INFO.S3MultiPart.ConsumerFinished", consumerId, ( System.currentTimeMillis() - consumerStart ) ) );
          } catch ( Exception ce ) {
            consumerFailed.set( true );
            consumerException[ 0 ] = ce;
            logger.error( BaseMessages.getString( PKG, "ERROR.S3MultiPart.ConsumerFailed" ), consumerId, ce );
            throw ce;
          }
          return localEtags;
        } ) );
      }
      return partFutures;
    }
  }

  /**
   * Buffer for a single S3 part, including part data, part number, offset, and length.
   */
  static class PartBuffer {
    final byte[] partBytes;
    final int partNum;
    final long offset;
    final int length;
    PartBuffer( byte[] partBytes, int partNum, long offset, int length ) {
      this.partBytes = partBytes;
      this.partNum = partNum;
      this.offset = offset;
      this.length = length;
    }
  }

  /**
   * Associates a PartETag with its part number for correct ordering.
   */
  static class PartETagWithNum {
    final PartETag partETag;
    final int partNum;
    PartETagWithNum( PartETag partETag, int partNum ) {
      this.partETag = partETag;
      this.partNum = partNum;
    }
  }
}
