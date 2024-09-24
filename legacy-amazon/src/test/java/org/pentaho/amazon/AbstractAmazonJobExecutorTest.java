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

package org.pentaho.amazon;

import com.amazonaws.auth.AWSCredentials;
import org.apache.commons.vfs2.FileObject;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.amazon.hive.job.AmazonHiveJobExecutor;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Created by Aliaksandr_Zhuk on 2/7/2018.
 */
@RunWith( MockitoJUnitRunner.class )
public class AbstractAmazonJobExecutorTest {
  @ClassRule public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File stagingFile;
  private File stagingFolder;
  private AbstractAmazonJobExecutor jobExecutor;

  @Before
  public void setUp() throws Exception {
    jobExecutor = spy( new AmazonHiveJobExecutor() );
    stagingFolder = temporaryFolder.newFolder( "emr" );
    stagingFile = temporaryFolder.newFile( stagingFolder.getName() + "/hive.q" );
  }

  @Test
  public void testGetS3FileObjectPath_validPath() throws Exception {

    String stagingDirWithScheme = "s3://s3/emr/hive";
    String expectedStagingDirPath = "/s3/emr/hive";

    AWSCredentials credentials = mock( AWSCredentials.class );
    jobExecutor.stagingDir = stagingDirWithScheme;
    when( jobExecutor.getS3FileObjectPath() ).thenCallRealMethod();

    String stagingDirPath = jobExecutor.getS3FileObjectPath();

    assertEquals( expectedStagingDirPath, stagingDirPath );
  }

  @Test
  public void testGetKeyFromS3StagingDir_getNullKey() throws Exception {

    when( jobExecutor.getS3FileObjectPath() ).thenReturn( "/test" );
    when( jobExecutor.getKeyFromS3StagingDir() ).thenCallRealMethod();

    String bucketKey = jobExecutor.getKeyFromS3StagingDir();

    assertEquals( null, bucketKey );
  }

  @Test
  public void testGetKeyFromS3StagingDir_getNotNullKey() throws Exception {

    when( jobExecutor.getS3FileObjectPath() ).thenReturn(  "/bucket/key" );
    when( jobExecutor.getKeyFromS3StagingDir() ).thenCallRealMethod();

    String bucketKey = jobExecutor.getKeyFromS3StagingDir();

    assertEquals( "key", bucketKey );
  }

  @Test
  public void testSetS3BucketKey_keyNotNull() throws Exception {

    String bucketKey = "key/subkey";
    String expectedKey = bucketKey + "/" + stagingFile.getName();

    FileObject stagingFileObject = KettleVFS.getFileObject( stagingFile.getPath() );

    when( jobExecutor.getKeyFromS3StagingDir() ).thenReturn( bucketKey );
    doCallRealMethod().when( jobExecutor ).setS3BucketKey( any() );

    jobExecutor.setS3BucketKey( stagingFileObject );

    String bucketKeyWithFileName = jobExecutor.key;

    assertEquals( expectedKey, bucketKeyWithFileName );
  }

  @Test
  public void testSetS3BucketKey_keyNull() throws Exception {

    String bucketKey = null;
    String expectedKey = stagingFile.getName();

    FileObject stagingFileObject = KettleVFS.getFileObject( stagingFile.getPath() );

    when( jobExecutor.getKeyFromS3StagingDir() ).thenReturn( bucketKey );
    doCallRealMethod().when( jobExecutor ).setS3BucketKey( any() );

    jobExecutor.setS3BucketKey( stagingFileObject );

    String bucketKeyWithFileName = jobExecutor.key;

    assertEquals( expectedKey, bucketKeyWithFileName );
  }

  @Test
  public void testSetS3BucketKey_keyEmptyString() throws Exception {

    String bucketKey = "";
    String expectedKey = stagingFile.getName();

    FileObject stagingFileObject = KettleVFS.getFileObject( stagingFile.getPath() );

    when( jobExecutor.getKeyFromS3StagingDir() ).thenReturn( bucketKey );
    doCallRealMethod().when( jobExecutor ).setS3BucketKey( any() );

    jobExecutor.setS3BucketKey( stagingFileObject );

    String bucketKeyWithFileName = jobExecutor.key;

    assertEquals( expectedKey, bucketKeyWithFileName );
  }

  @Test
  public void testGetStagingBucketName_OneFolder() throws Exception {

    String expectedBucketName = "test";

    when( jobExecutor.getS3FileObjectPath() ).thenReturn( "/test" );

    String bucketName = jobExecutor.getStagingBucketName();

    assertEquals( expectedBucketName, bucketName );
  }

  @Test
  public void testGetStagingBucketName_withSubfolder() throws Exception {

    String expectedBucketName = "test";

    when( jobExecutor.getS3FileObjectPath() ).thenReturn( "/test/hive" );

    String bucketName = jobExecutor.getStagingBucketName();

    assertEquals( expectedBucketName, bucketName );
  }
}
