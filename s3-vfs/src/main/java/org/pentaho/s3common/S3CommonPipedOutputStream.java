/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright ( C 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.s3common;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StorageUnitConverter;
import org.pentaho.di.i18n.BaseMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;

@SuppressWarnings( "UseSpecificCatch" ) // We need to catch Exception to handle all exceptions
public class S3CommonPipedOutputStream extends PipedOutputStream {

  private static final Class<?> PKG = S3CommonPipedOutputStream.class;
  private static final Logger logger = LoggerFactory.getLogger( S3CommonPipedOutputStream.class );
  private static final LogChannelInterface consoleLog = new LogChannel( BaseMessages.getString( PKG, "TITLE.S3File" ) );

  public static final int DEFAULT_PART_SIZE = 128 * 1024 * 1024; // Minimum part size is 5MB, but we use 128MB for better performance
  public static final int DEFAULT_THREAD_POOL_SIZE = 4; // Default thread pool size for consumers
  public static final byte [] POISON_PILL = new byte [0];

  private ExecutorService controllerExecutor;
  private ExecutorService consumerExecutor;
  private ExecutorService producerExecutor;
  private boolean initialized = false;
  private boolean blockedUntilDone = true;
  private PipedInputStream pipedInputStream;
  private S3AsyncTransferRunner s3AsyncTransferRunner;
  private S3CommonFileSystem fileSystem;
  private Future<Boolean> result = null;
  private String bucketId;
  private String key;
  private int partSize;
  private int threadPoolSize;

  public S3CommonPipedOutputStream( S3CommonFileSystem fileSystem, String bucketId, String key ) throws IOException {
    this( fileSystem, bucketId, key, DEFAULT_PART_SIZE, DEFAULT_THREAD_POOL_SIZE );
  }

  public S3CommonPipedOutputStream( S3CommonFileSystem fileSystem, String bucketId, String key,
      int partSize, int threadPoolSize ) throws IOException {
    if ( partSize < 5 * 1024 * 1024 ) { // S3 minimum part size is 5MB
      throw new IllegalArgumentException( "partSize must be at least 5MB" );
    }
    if ( threadPoolSize < 1 ) {
      throw new IllegalArgumentException( "threadPoolSize must be at least 1" );
    }
    this.pipedInputStream = new PipedInputStream( this );
    this.s3AsyncTransferRunner = new S3AsyncTransferRunner();
    this.bucketId = bucketId;
    this.key = key;
    this.fileSystem = fileSystem;
    this.partSize = partSize;
    this.threadPoolSize = threadPoolSize;
    this.controllerExecutor = Executors.newSingleThreadExecutor();
    this.consumerExecutor = Executors.newFixedThreadPool( threadPoolSize );
    this.producerExecutor = Executors.newSingleThreadExecutor();
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
  public void write( byte [] b, int off, int len ) throws IOException {
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
      if ( !consumerTerminated && logger.isWarnEnabled() ) {
        logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.ConsumerNotTerminated" ) );
      }
    } catch ( InterruptedException e ) {
      logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.InterruptedConsumerShutdown" ), e );
      Thread.currentThread().interrupt();
    }
    try {
      boolean controllerTerminated = controllerExecutor.awaitTermination( 60, java.util.concurrent.TimeUnit.SECONDS );
      if ( !controllerTerminated && logger.isWarnEnabled() ) {
        logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.ControllerNotTerminated" ) );
      }
    } catch ( InterruptedException e ) {
      logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.InterruptedControllerShutdown" ), e );
      Thread.currentThread().interrupt();
    }
    if ( thrown != null ) {
      throw thrown;
    }
  }

  class S3AsyncTransferRunner implements Callable<Boolean> {
    private class PartBufferState {
      byte[] partBuffer;
      int partBufferPos;
      int partNum;
      long offset;
      PartBufferState( byte[] partBuffer, int partBufferPos, int partNum, long offset ) {
        this.partBuffer = partBuffer;
        this.partBufferPos = partBufferPos;
        this.partNum = partNum;
        this.offset = offset;
      }
    }

    public Boolean call() {
      boolean returnVal = true;
      int queueCapacity = Math.max( threadPoolSize * 2, 4 );
      List<Future<List<PartETagWithNum>>> partFutures = new ArrayList<>();
      List<PartETagWithNum> partETagsWithNum = new ArrayList<>();
      InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest( bucketId, key );
      InitiateMultipartUploadResult initResponse = null;
      BlockingQueue<PartBuffer> queue = new LinkedBlockingQueue<>( queueCapacity );
      AtomicBoolean producerFailed = new AtomicBoolean( false );
      AtomicBoolean consumerFailed = new AtomicBoolean( false );
      Throwable [] consumerException = new Throwable [1];
      long startTime = System.currentTimeMillis();

      try {
        initResponse = fileSystem.getS3Client().initiateMultipartUpload( initRequest );
        final InitiateMultipartUploadResult finalInitResponse = initResponse;
        partFutures = startConsumers( queue, finalInitResponse, consumerFailed, consumerException );
        runProducerAndWait( queue, producerFailed );
        if ( producerFailed.get() ) {
          throw new IOException( "Producer failed" );
        }
        handlePartFutures( partFutures, partETagsWithNum, consumerFailed, consumerException );
        if ( consumerFailed.get() && consumerException [0] != null ) {
          throw new IOException( "Consumer failed", consumerException [0] );
        }
        completeMultipartUpload( partETagsWithNum, finalInitResponse, startTime );
      } catch ( OutOfMemoryError oome ) {
        handleOutOfMemory( oome );
        returnVal = false;
      } catch ( Exception e ) {
        handleException( e, initResponse );
        returnVal = false;
      } finally {
        producerExecutor.shutdown();
        consumerExecutor.shutdown();
      }
      return returnVal;
    }

    private void runProducerAndWait( BlockingQueue<PartBuffer> queue, AtomicBoolean producerFailed )
        throws InterruptedException, ExecutionException {
      Future<?> producerFuture = producerExecutor.submit( () -> createProducer( queue, producerFailed ).run() );
      producerFuture.get();
    }

    private void handlePartFutures( List<Future<List<PartETagWithNum>>> partFutures, List<PartETagWithNum> partETagsWithNum,
                                    AtomicBoolean consumerFailed, Throwable[] consumerException ) {
      boolean shouldBreak = false;
      for ( Future<List<PartETagWithNum>> f: partFutures ) {
        if ( shouldBreak ) {
          break;
        }
        try {
          partETagsWithNum.addAll( f.get() );
        } catch ( InterruptedException ie ) {
          Thread.currentThread().interrupt();
          consumerFailed.set( true );
          consumerException [0] = ie;
          shouldBreak = true;
        } catch ( Exception ce ) {
          if ( logger != null && logger.isErrorEnabled() ) {
            logger.error( BaseMessages.getString( PKG, "ERROR.S3MultiPart.ExceptionWhileWaitingForPartFuture" ), ce );
          }
          consumerFailed.set( true );
          consumerException [0] = ce;
          shouldBreak = true;
        }
      }
    }

    private void completeMultipartUpload( List<PartETagWithNum> partETagsWithNum,
        InitiateMultipartUploadResult finalInitResponse, long startTime ) {
      partETagsWithNum.sort( ( a, b ) -> Integer.compare( a.partNum, b.partNum ) );
      List<PartETag> partETags = new ArrayList<>();
      for ( PartETagWithNum p: partETagsWithNum ) {
        partETags.add( p.partETag );
      }
      if ( logger != null && logger.isInfoEnabled() ) {
        logger.info( BaseMessages.getString( PKG, "INFO.S3MultiPart.Complete" ) );
        logger.info( BaseMessages.getString( PKG, "INFO.S3MultiPart.TotalUploadTime", ( System.currentTimeMillis() - startTime ) ) );
      }
      CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest( bucketId, key,
          finalInitResponse.getUploadId(), partETags );
      fileSystem.getS3Client().completeMultipartUpload( compRequest );
    }

    private void handleOutOfMemory( OutOfMemoryError oome ) {
      consoleLog.logError( BaseMessages.getString( PKG,
          "ERROR.S3MultiPart.UploadOutOfMemory", new StorageUnitConverter().byteCountToDisplaySize( partSize ) ),
          oome );
    }

    private void handleException( Exception e, InitiateMultipartUploadResult initResponse ) {
      if ( logger != null && logger.isErrorEnabled() ) {
        logger.error( BaseMessages.getString( PKG, "ERROR.S3MultiPart.ExceptionCaught" ), e );
      }
      try {
        if ( initResponse == null ) {
          close();
        } else {
          fileSystem.getS3Client().abortMultipartUpload( new AbortMultipartUploadRequest( bucketId, key, initResponse.getUploadId() ) );
        }
      } catch ( Exception abortEx ) {
        if ( logger != null && logger.isErrorEnabled() ) {
          logger.error( BaseMessages.getString( PKG, "ERROR.S3MultiPart.AbortOrClose" ), abortEx );
        }
      }
    }

    private Runnable createProducer( BlockingQueue<PartBuffer> queue, AtomicBoolean producerFailed ) {
      return () -> {
        long producerStart = System.currentTimeMillis();
        try ( BufferedInputStream bis = new BufferedInputStream( pipedInputStream, partSize ) ) {
          produceParts( bis, queue, producerFailed );
          insertPoisonPills( queue );
          if ( logger.isInfoEnabled() ) {
            logger.info( BaseMessages.getString( PKG, "INFO.S3MultiPart.ProducerFinished",
              ( System.currentTimeMillis() - producerStart ) ) );
          }
        } catch ( Exception e ) {
          producerFailed.set( true );
          handleProducerException( queue, e );
        }
      };
    }

    private void produceParts( BufferedInputStream bis, BlockingQueue<PartBuffer> queue, AtomicBoolean producerFailed )
        throws IOException, InterruptedException {
      PartBufferState state = new PartBufferState( new byte[partSize], 0, 1, 0 );
      byte[] tmpBuffer = new byte[64 * 1024];
      int read;
      while ( ( read = bis.read( tmpBuffer ) ) >= 0 ) {
        state = fillPartBufferAndQueue( tmpBuffer, read, state, queue, producerFailed );
        if ( state == null ) {
          return; // too many parts, abort
        }
      }
      queueFinalPartialPart( state.partBuffer, state.partBufferPos, state.partNum, state.offset, queue, producerFailed );
    }

    private PartBufferState fillPartBufferAndQueue( byte[] tmpBuffer, int read, PartBufferState state,
        BlockingQueue<PartBuffer> queue, AtomicBoolean producerFailed ) throws InterruptedException {
      int srcPos = 0;
      byte[] partBuffer = state.partBuffer;
      int partBufferPos = state.partBufferPos;
      int partNum = state.partNum;
      long offset = state.offset;
      while ( srcPos < read ) {
        int space = partSize - partBufferPos;
        int copyLen = Math.min( space, read - srcPos );
        System.arraycopy( tmpBuffer, srcPos, partBuffer, partBufferPos, copyLen );
        partBufferPos += copyLen;
        srcPos += copyLen;
        if ( partBufferPos == partSize ) {
          if ( handleTooManyParts( partNum, queue, producerFailed ) ) {
            return null;
          }
          if ( logger != null && logger.isDebugEnabled() ) {
            logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ProducerPuttingPart", partNum, offset, partSize ) );
          }
          queue.put( new PartBuffer( partBuffer, partNum, offset, partSize ) );
          partNum++;
          offset += partSize;
          partBuffer = new byte[partSize];
          partBufferPos = 0;
        }
      }
      return new PartBufferState( partBuffer, partBufferPos, partNum, offset );
    }

    private void queueFinalPartialPart( byte [] partBuffer, int partBufferPos, int partNum, long offset,
        BlockingQueue<PartBuffer> queue, AtomicBoolean producerFailed ) throws InterruptedException {
      if ( partBufferPos > 0 ) {
        if ( handleTooManyParts( partNum, queue, producerFailed ) ) {
          return;
        }
        byte [] lastPart = java.util.Arrays.copyOf( partBuffer, partBufferPos );
        if ( logger != null && logger.isDebugEnabled() ) {
          logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ProducerPuttingFinalPart", partNum, offset,
              partBufferPos ) );
        }
        queue.put( new PartBuffer( lastPart, partNum, offset, partBufferPos ) );
      }
    }

    private boolean handleTooManyParts( int partNum, BlockingQueue<PartBuffer> queue, AtomicBoolean producerFailed )
        throws InterruptedException {
      if ( partNum > 10000 ) {
        if ( logger != null && logger.isErrorEnabled() ) {
          logger.error( BaseMessages.getString( PKG, "ERROR.S3MultiPart.TooManyParts" ), partNum );
        }
        producerFailed.set( true );
        insertPoisonPills( queue );
        return true;
      }
      return false;
    }

    private void insertPoisonPills( BlockingQueue<PartBuffer> queue ) throws InterruptedException {
      for ( int i = 0; i < threadPoolSize; i++ ) {
        boolean inserted = false;
        while ( !inserted ) {
          inserted = queue.offer( new PartBuffer( POISON_PILL, -1, -1, -1 ), 5, java.util.concurrent.TimeUnit.SECONDS );
          if ( !inserted && logger != null && logger.isWarnEnabled() ) {
            logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.WaitingToInsertPoisonPill", i ) );
          }
        }
        if ( logger != null && logger.isDebugEnabled() ) {
          logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.InsertedPoisonPill", i ) );
        }
      }
    }

    private void handleProducerException( BlockingQueue<PartBuffer> queue, Exception e ) {
      insertPoisonPillsWithLogging( queue );
      if ( logger != null && logger.isErrorEnabled() ) {
        logger.error( BaseMessages.getString( PKG, "ERROR.S3MultiPart.ProducerFailed" ), e );
      }
    }

    private void insertPoisonPillsWithLogging( BlockingQueue<PartBuffer> queue ) {
      for ( int i = 0; i < threadPoolSize; i++ ) {
        insertSinglePoisonPillWithLogging( queue, i );
      }
    }

    private void insertSinglePoisonPillWithLogging( BlockingQueue<PartBuffer> queue, int i ) {
      boolean inserted = false;
      while ( !inserted ) {
        try {
          inserted = queue.offer( new PartBuffer( POISON_PILL, -1, -1, -1 ), 5, java.util.concurrent.TimeUnit.SECONDS );
          if ( !inserted && logger != null && logger.isWarnEnabled() ) {
            logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.WaitingToInsertPoisonPill", i ) );
          }
        } catch ( InterruptedException ie ) {
          if ( logger != null && logger.isWarnEnabled() ) {
            logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.ProducerInterrupted" ), ie );
          }
          Thread.currentThread().interrupt();
          break;
        }
      }
      if ( logger != null && logger.isDebugEnabled() ) {
        logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ProducerInsertedPoisonPillOnError", i ) );
      }
    }

    private List<Future<List<PartETagWithNum>>> startConsumers( BlockingQueue<PartBuffer> queue,
        InitiateMultipartUploadResult finalInitResponse, AtomicBoolean consumerFailed, Throwable[] consumerException ) {
      List<Future<List<PartETagWithNum>>> partFutures = new ArrayList<>();
      for ( int i = 0; i < threadPoolSize; i++ ) {
        final int consumerId = i;
        partFutures.add( consumerExecutor.submit( () -> consumerTask( queue, finalInitResponse, consumerId, consumerFailed, consumerException ) ) );
      }
      return partFutures;
    }

    private List<PartETagWithNum> consumerTask( BlockingQueue<PartBuffer> queue, InitiateMultipartUploadResult finalInitResponse, int consumerId,
                                                AtomicBoolean consumerFailed, Throwable[] consumerException ) {
      List<PartETagWithNum> localEtags = new ArrayList<>();
      try {
        while ( true ) {
          logConsumerWaiting( consumerId );
          PartBuffer part = queue.take();
          if ( isPoisonPill( part, consumerId ) ) {
            break;
          }
          logConsumerGotPart( consumerId, part );
          localEtags.add( uploadPartAndGetETag( part, finalInitResponse ) );
        }
        logConsumerFinished( consumerId );
      } catch ( Exception ce ) {
        handleConsumerException( ce, consumerId, consumerFailed, consumerException );
        if ( ce instanceof InterruptedException ) {
          // Already interrupted, just return to exit the thread cleanly
          return localEtags;
        }
        if ( ce instanceof RuntimeException ) {
          throw (RuntimeException) ce;
        } else {
          throw new S3ConsumerException( ce );
        }
      }
      return localEtags;
    }

    private void logConsumerWaiting( int consumerId ) {
      if ( logger != null && logger.isDebugEnabled() ) {
        logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ConsumerWaitingForPart", consumerId ) );
      }
    }

    private boolean isPoisonPill( PartBuffer part, int consumerId ) {
      if ( part.partBytes == POISON_PILL ) {
        if ( logger != null && logger.isDebugEnabled() ) {
          logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ConsumerReceivedPoisonPill", consumerId ) );
        }
        return true;
      }
      return false;
    }

    private void logConsumerGotPart( int consumerId, PartBuffer part ) {
      if ( logger != null && logger.isDebugEnabled() ) {
        logger.debug( BaseMessages.getString( PKG, "DEBUG.S3MultiPart.ConsumerGotPart", consumerId, part.partNum, part.offset, part.length ) );
      }
    }

    private void logConsumerFinished( int consumerId ) {
      if ( logger != null && logger.isInfoEnabled() ) {
        logger.info( BaseMessages.getString( PKG, "INFO.S3MultiPart.ConsumerFinished", consumerId ) );
      }
    }

    private void handleConsumerException( Exception ce, int consumerId, AtomicBoolean consumerFailed, Throwable [] consumerException ) {
      if ( ce instanceof InterruptedException ) {
        Thread.currentThread().interrupt();
      }
      consumerFailed.set( true );
      consumerException [0] = ce;
      if ( logger != null && logger.isErrorEnabled() ) {
        logger.error( BaseMessages.getString( PKG, "ERROR.S3MultiPart.ConsumerFailed" ), consumerId, ce );
      }
    }

    private PartETagWithNum uploadPartAndGetETag( PartBuffer part, InitiateMultipartUploadResult finalInitResponse ) {
      S3CommonWindowedSubstream s3is = new S3CommonWindowedSubstream( part.partBytes );
      UploadPartRequest uploadRequest = new UploadPartRequest()
          .withBucketName( bucketId ).withKey( key )
          .withUploadId( finalInitResponse.getUploadId() ).withPartNumber( part.partNum )
          .withFileOffset( part.offset )
          .withPartSize( part.length )
          .withInputStream( s3is )
          .withLastPart( false );
      if ( logger != null && logger.isInfoEnabled() ) {
        logger.info( BaseMessages.getString( PKG, "INFO.S3MultiPart.Upload", part.partNum, part.offset, part.length ) );
      }
      PartETag etag = fileSystem.getS3Client().uploadPart( uploadRequest ).getPartETag();
      return new PartETagWithNum( etag, part.partNum );
    }
  }

  /**
   * Buffer for a single S3 part, including part data, part number, offset, and
   * length.
   */
  static class PartBuffer {
    final byte [] partBytes;
    final int partNum;
    final long offset;
    final int length;

    PartBuffer( byte [] partBytes, int partNum, long offset, int length ) {
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

  static class S3ConsumerException extends RuntimeException {
    S3ConsumerException( Throwable cause ) {
      super( cause );
    }
  }
}
