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


package org.pentaho.s3.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StorageUnitConverter;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.s3common.S3CommonFileSystem;
import org.pentaho.s3common.S3KettleProperty;

import static org.pentaho.s3common.S3CommonPipedOutputStream.DEFAULT_THREAD_POOL_SIZE;

public class S3FileSystem extends S3CommonFileSystem {

  private static final Class<?> PKG = S3FileSystem.class;
  private static final LogChannelInterface consoleLog = new LogChannel( BaseMessages.getString( PKG, "TITLE.S3File" ) );

  protected StorageUnitConverter storageUnitConverter;
  protected S3KettleProperty s3KettleProperty;

  /**
   * Minimum part size specified in documentation
   * see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
   */
  private static final String MIN_PART_SIZE = "5MB";
  /**
   * Maximum part size specified in documentation
   * see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
   */
  private static final String MAX_PART_SIZE = "5GB";

  private static final StorageUnitConverter STATIC_STORAGE_UNIT_CONVERTER = new StorageUnitConverter();
  private static final long MIN_PART_SIZE_BYTES = STATIC_STORAGE_UNIT_CONVERTER.displaySizeToByteCount( MIN_PART_SIZE );
  private static final long MAX_PART_SIZE_BYTES = STATIC_STORAGE_UNIT_CONVERTER.displaySizeToByteCount( MAX_PART_SIZE );

  protected S3FileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions ) {
    this( rootName, fileSystemOptions, STATIC_STORAGE_UNIT_CONVERTER, new S3KettleProperty() );
  }

  protected S3FileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions,
                          final StorageUnitConverter storageUnitConverter, final S3KettleProperty s3KettleProperty ) {
    super( rootName, fileSystemOptions );
    this.storageUnitConverter = storageUnitConverter;
    this.s3KettleProperty = s3KettleProperty;
  }

  protected FileObject createFile( AbstractFileName name ) {
    return new S3FileObject( name, this );
  }

  public int getPartSize() {
    long parsedPartSize = parsePartSize( s3KettleProperty.getPartSize() );
    return convertToInt( parsedPartSize );
  }

  protected long parsePartSize( String partSizeString ) {
    long parsePartSize = convertToLong( partSizeString );
    if ( parsePartSize < MIN_PART_SIZE_BYTES ) {
      consoleLog.logBasic( BaseMessages.getString( PKG, "WARN.S3MultiPart.DefaultPartSize", partSizeString, MIN_PART_SIZE ) );
      parsePartSize = MIN_PART_SIZE_BYTES;
    }

    // still allow > 5GB, api might be updated in the future
    if ( parsePartSize > MAX_PART_SIZE_BYTES ) {
      consoleLog.logBasic( BaseMessages.getString( PKG, "WARN.S3MultiPart.MaximumPartSize", partSizeString, MAX_PART_SIZE ) );
    }
    return parsePartSize;
  }

  protected int convertToInt( long parsedPartSize ) {
    return (int) Long.min( Integer.MAX_VALUE, parsedPartSize );
  }

  protected long convertToLong( String partSize ) {
    return storageUnitConverter.displaySizeToByteCount( partSize );
  }

  public int getThreadPoolSize() {
    String poolSizeStr = s3KettleProperty.getThreadPoolSize();
    int poolSize = DEFAULT_THREAD_POOL_SIZE;
    try {
      if ( poolSizeStr != null && !poolSizeStr.isEmpty() ) {
        poolSize = Integer.parseInt( poolSizeStr );
        if ( poolSize < 1 ) {
          consoleLog.logBasic( BaseMessages.getString( PKG, "WARN.S3MultiPart.DefaultThreadPoolSize", poolSizeStr, Integer.toString( DEFAULT_THREAD_POOL_SIZE ) ) );
          poolSize = DEFAULT_THREAD_POOL_SIZE;
        }
      }
    } catch ( NumberFormatException e ) {
      consoleLog.logBasic( BaseMessages.getString( PKG, "WARN.S3MultiPart.InvalidThreadPoolSize", poolSizeStr, Integer.toString( DEFAULT_THREAD_POOL_SIZE ) ) );
      poolSize = DEFAULT_THREAD_POOL_SIZE;
    }
    return poolSize;
  }
}
