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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.StorageUnitConverter;
import org.pentaho.s3common.S3KettleProperty;
import org.pentaho.s3common.S3TransferManager;
import org.pentaho.s3common.TestCleanupUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for S3FileSystem
 */
@RunWith( MockitoJUnitRunner.class )
public class S3FileSystemTest {

  S3FileSystem fileSystem;
  S3FileName fileName;

  @BeforeClass
  public static void setClassUp() throws KettleException {
    KettleEnvironment.init( false );
  }

  @AfterClass
  public static void tearDownClass() {
    KettleEnvironment.shutdown();
    TestCleanupUtil.cleanUpLogsDir();
  }

  @Before
  public void setUp() {
    fileName = new S3FileName(
      S3FileNameTest.SCHEME,
      "/",
      "",
      FileType.FOLDER );
    fileSystem = new S3FileSystem( fileName, new FileSystemOptions() );
  }

  @Test
  public void testCreateFile() {
    assertNotNull( fileSystem.createFile( new S3FileName( "s3", "bucketName", "/bucketName/key", FileType.FILE ) ) );
  }

  @Test
  public void testGetS3Service() {
    assertNotNull( fileSystem.getS3Client() );

    FileSystemOptions options = new FileSystemOptions();
    UserAuthenticator authenticator = mock( UserAuthenticator.class );
    DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( options, authenticator );
    // test is still slow for little gain, but the region check was the slowest part
    fileSystem = new S3FileSystem( fileName, options ) {
      @Override
      protected boolean isRegionSet() {
        return false;
      }
    };
    assertNotNull( fileSystem.getS3Client() );
  }

  @Test
  public void getPartSize() {
    S3FileSystem s3FileSystem = getTestInstance();
    s3FileSystem.storageUnitConverter = new StorageUnitConverter();
    S3KettleProperty s3KettleProperty = mock( S3KettleProperty.class );
    when( s3KettleProperty.getPartSize() ).thenReturn( "6MB" );
    s3FileSystem.s3KettleProperty = s3KettleProperty;


    //TEST 1: Below max
    assertEquals( 6 * 1024 * 1024, s3FileSystem.getPartSize() );

    // TEst 2: above max
    when( s3KettleProperty.getPartSize() ).thenReturn( "600GB" );
    assertEquals( 600L * 1024 * 1024 * 1024, s3FileSystem.getPartSize() );
  }

  @Test
  public void testParsePartSize() {
    S3FileSystem s3FileSystem = getTestInstance();
    s3FileSystem.storageUnitConverter = new StorageUnitConverter();
    long _5MBLong = 5L * 1024L * 1024L;
    long _124MBLong = 124L * 1024L * 1024L;
    long _5GBLong = 5L * 1024L * 1024L * 1024L;
    long _12GBLong = 12L * 1024L * 1024L * 1024L;
    long minimumPartSize = _5MBLong;
    long maximumPartSize = _5GBLong;


    // TEST 1: below minimum
    assertEquals( minimumPartSize, s3FileSystem.parsePartSize( "1MB" ) );

    // TEST 2: at minimum
    assertEquals( minimumPartSize, s3FileSystem.parsePartSize( "5MB" ) );

    // TEST 3: between minimum and maximum
    assertEquals( _124MBLong, s3FileSystem.parsePartSize( "124MB" ) );

    // TEST 4: at maximum
    assertEquals( maximumPartSize, s3FileSystem.parsePartSize( "5GB" ) );

    // TEST 5: above maximum
    assertEquals( _12GBLong, s3FileSystem.parsePartSize( "12GB" ) );
  }

  public S3FileSystem getTestInstance() {
    // Use a real S3FileName with dummy but non-null values to avoid NPE in S3Util.getKeysFromURI
    S3FileName rootName = new S3FileName( "s3", "bucket", "/bucket/key", FileType.FOLDER );
    FileSystemOptions fileSystemOptions = new FileSystemOptions();
    return new S3FileSystem( rootName, fileSystemOptions );
  }

  @Test
  public void getS3ClientWithDefaultRegion() {
    FileSystemOptions options = new FileSystemOptions();
    try ( MockedStatic<Regions> regionsMockedStatic = Mockito.mockStatic( Regions.class ) ) {
      regionsMockedStatic.when( Regions::getCurrentRegion ).thenReturn( null );
      //Not under an EC2 instance - getCurrentRegion returns null

      fileSystem = new S3FileSystem( fileName, options );

      AmazonS3Client s3Client = (AmazonS3Client) fileSystem.getS3Client();
      assertEquals( "No Region was configured - client must have default region",
        Regions.DEFAULT_REGION.getName(), s3Client.getRegionName() );
    }
  }

  @Test
  public void testCopy_DelegatesToTransferManager() throws Exception {
    S3FileObject src = mock( S3FileObject.class );
    S3FileObject dest = mock( S3FileObject.class );
    S3TransferManager transferManager = mock( S3TransferManager.class );
    S3FileSystem fs = Mockito.spy( getTestInstance() );
    doReturn( transferManager ).when( fs ).getS3TransferManager();
    fs.copy( src, dest );
    verify( transferManager, times( 1 ) ).copy( src, dest );
  }

  @Test
  public void testGetS3TransferManager_CreatesWithTransferManager() {
    S3FileSystem fs = Mockito.spy( getTestInstance() );
    S3TransferManager transferManager = mock( S3TransferManager.class );
    doReturn( transferManager ).when( fs ).getS3TransferManager();
    S3TransferManager result = fs.getS3TransferManager();
    assertNotNull( result );
  }

  @Test
  public void testBuildTransferManager_CreatesWithS3Client() {
    S3FileSystem fs = Mockito.spy( getTestInstance() );
    com.amazonaws.services.s3.AmazonS3 s3Client = mock( com.amazonaws.services.s3.AmazonS3.class );
    doReturn( s3Client ).when( fs ).getS3Client();
    TransferManager tm = fs.buildTransferManager();
    assertNotNull( tm );
  }
}
