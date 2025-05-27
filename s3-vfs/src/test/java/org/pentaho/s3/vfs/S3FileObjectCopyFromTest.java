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

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.di.core.util.StorageUnitConverter;
import org.pentaho.s3common.S3KettleProperty;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3FileObjectCopyFromTest {

  private S3FileObject dst;
  private S3FileObject src;
  private FileSelector selector;
  private S3FileSystem fileSystem;

  @Before
  public void setUp() {
    S3FileName fileName = new S3FileName( "s3", "bucket", "/bucket/key", org.apache.commons.vfs2.FileType.FILE );
    S3KettleProperty kettleProperty = new S3KettleProperty();
    StorageUnitConverter storageUnitConverter = new StorageUnitConverter();
    S3FileSystem realFileSystem = new S3FileSystem( fileName, new org.apache.commons.vfs2.FileSystemOptions(), storageUnitConverter, kettleProperty );
    fileSystem = Mockito.spy( realFileSystem );
    dst = Mockito.spy( new S3FileObject( fileName, fileSystem ) );
    src = Mockito.spy( new S3FileObject( fileName, fileSystem ) );
    selector = mock( FileSelector.class );
  }

  @After
  public void tearDown() {
    dst = null;
    src = null;
    selector = null;
    fileSystem = null;
  }

  @Test
  public void testCopyFrom_S3ToS3_Success() throws FileSystemException {
    // Arrange: dst.fileSystem.copy should succeed
    doNothing().when( fileSystem ).copy( any( S3FileObject.class ), any( S3FileObject.class ) );
    // Act
    dst.copyFrom( src, selector );
    // Assert: fileSystem.copy was called
    verify( fileSystem, times( 1 ) ).copy( any( S3FileObject.class ), any( S3FileObject.class ) );
  }

  @Test
  public void testCopyFrom_NonS3Source_FallbackToDefault() throws FileSystemException {
    FileObject nonS3 = mock( FileObject.class );
    when( nonS3.exists() ).thenReturn( true );
    // Act
    dst.copyFrom( nonS3, selector );
    // Assert: fileSystem.copy was not called and fileSystem.upload was called
    verify( fileSystem, times( 0 ) ).copy( any( S3FileObject.class ), any( S3FileObject.class ) );
    verify( fileSystem, times( 1 ) ).upload( any( FileObject.class ), any( S3FileObject.class ) );
  }

  @Test
  public void testCopyFrom_S3ToS3_Exception_FallbackToDefault() throws FileSystemException {
    // Arrange: fileSystem.copy(S3FileObject, S3FileObject) throws, should fallback to default
    doThrow( new FileSystemException( "fail" ) ).when( fileSystem ).copy( any( S3FileObject.class ), any( S3FileObject.class ) );
    FileSelector sel = mock( FileSelector.class );
    doReturn( org.apache.commons.vfs2.FileType.FILE ).when( dst ).getType();
    doReturn( org.apache.commons.vfs2.FileType.FILE ).when( src ).getType();
    doReturn( true ).when( dst ).exists();
    doReturn( true ).when( src ).exists();
    dst.copyFrom( src, sel );
    // Assert: fileSystem.copy was called once and fallback to fileSystem.upload was called
    verify( fileSystem, times( 1 ) ).copy( any( S3FileObject.class ), any( S3FileObject.class ) );
    verify( fileSystem, times( 1 ) ).upload( any( FileObject.class ), any( S3FileObject.class ) );
  }
}
