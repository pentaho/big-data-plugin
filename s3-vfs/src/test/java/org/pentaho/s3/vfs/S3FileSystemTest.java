/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.s3.vfs;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.util.StorageUnitConverter;
import org.pentaho.s3common.S3CommonFileSystemTestUtil;
import org.pentaho.s3common.S3KettleProperty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for S3FileSystem
 */
@RunWith( MockitoJUnitRunner.class )
public class S3FileSystemTest {

  S3FileSystem fileSystem;
  S3FileName fileName;

  @BeforeClass
  public static void initKettle() throws Exception {
    KettleEnvironment.init( false );
  }

  @Before
  public void setUp() throws Exception {
    fileName = new S3FileName(
      S3FileNameTest.SCHEME,
      "/",
      "",
      FileType.FOLDER );
    fileSystem = new S3FileSystem( fileName, new FileSystemOptions() );
  }

  @Test
  public void testCreateFile() throws Exception {
    assertNotNull( fileSystem.createFile( new S3FileName( "s3", "bucketName", "/bucketName/key", FileType.FILE ) ) );
  }

  @Test
  public void testGetS3Service() throws Exception {
    assertNotNull( fileSystem.getS3Client() );

    FileSystemOptions options = new FileSystemOptions();
    UserAuthenticator authenticator = mock( UserAuthenticator.class );
    DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( options, authenticator );

    fileSystem = new S3FileSystem( fileName, options );
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
    assertEquals( Integer.MAX_VALUE, s3FileSystem.getPartSize() );

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

  @Test
  public void testConvertToInt() {
    S3FileSystem s3FileSystem = getTestInstance();

    // TEST 1: below int max
    assertEquals( 10, s3FileSystem.convertToInt( 10L ) );

    // TEST 2: at int max
    assertEquals( Integer.MAX_VALUE, s3FileSystem.convertToInt( Integer.MAX_VALUE ) );

    // TEST 3: above int max
    assertEquals( Integer.MAX_VALUE, s3FileSystem.convertToInt( 5L * 1024L * 1024L * 1024L ) );
  }

  @Test
  public void testConvertToLong() {
    S3FileSystem s3FileSystem = getTestInstance();
    long _10MBLong = 10L * 1024L * 1024L;
    s3FileSystem.storageUnitConverter = new StorageUnitConverter();
    assertEquals( _10MBLong, s3FileSystem.convertToLong( "10MB" ) );
  }

  public S3FileSystem getTestInstance() {
    FileName rootName = mock( FileName.class );
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
      fileSystem = ( S3FileSystem ) S3CommonFileSystemTestUtil.stubRegionUnSet( fileSystem );

      AmazonS3Client s3Client = (AmazonS3Client) fileSystem.getS3Client();
      assertEquals( "No Region was configured - client must have default region",
        Regions.DEFAULT_REGION.getName(), s3Client.getRegionName() );
    }
  }
}
