/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon;

import com.amazonaws.auth.AWSCredentials;
import org.apache.commons.vfs2.FileObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.pentaho.amazon.hive.job.AmazonHiveJobExecutor;
import org.pentaho.di.core.vfs.KettleVFS;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by Aliaksandr_Zhuk on 2/7/2018.
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( AmazonHiveJobExecutor.class )
public class AbstractAmazonJobExecutorTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File stagingFile;
  private File stagingFolder;
  private AbstractAmazonJobExecutor jobExecutor;

  @Before
  public void setUp() throws Exception {
    jobExecutor = PowerMockito.spy( new AmazonHiveJobExecutor() );
    stagingFolder = temporaryFolder.newFolder( "emr" );
    stagingFile = temporaryFolder.newFile( stagingFolder.getName() + "/hive.q" );
  }

  @Test
  public void testGetS3FileObjectPath_validPath() throws Exception {

    String stagingDirWithScheme = "s3://s3/emr/hive";
    String expectedStagingDirPath = "/s3/emr/hive";

    AWSCredentials credentials = mock( AWSCredentials.class );
    when( credentials.getAWSAccessKeyId() ).thenReturn( null );
    when( credentials.getAWSSecretKey() ).thenReturn( null );
    Whitebox.setInternalState( jobExecutor, "stagingDir", stagingDirWithScheme );

    String stagingDirPath = org.powermock.reflect.Whitebox.invokeMethod( jobExecutor, "getS3FileObjectPath" );

    assertEquals( expectedStagingDirPath, stagingDirPath );
  }

  @Test
  public void testGetKeyFromS3StagingDir_getNullKey() throws Exception {

    PowerMockito.doReturn( "/test" ).when( jobExecutor, "getS3FileObjectPath" );

    String bucketKey = org.powermock.reflect.Whitebox.invokeMethod( jobExecutor, "getKeyFromS3StagingDir" );

    assertEquals( null, bucketKey );
  }

  @Test
  public void testGetKeyFromS3StagingDir_getNotNullKey() throws Exception {

    PowerMockito.doReturn( "/bucket/key" ).when( jobExecutor, "getS3FileObjectPath" );

    String bucketKey = org.powermock.reflect.Whitebox.invokeMethod( jobExecutor, "getKeyFromS3StagingDir" );

    assertEquals( "key", bucketKey );
  }

  @Test
  public void testSetS3BucketKey_keyNotNull() throws Exception {

    String bucketKey = "key/subkey";
    String expectedKey = bucketKey + "/" + stagingFile.getName();

    FileObject stagingFileObject = KettleVFS.getFileObject( stagingFile.getPath() );

    PowerMockito.doReturn( bucketKey ).when( jobExecutor, "getKeyFromS3StagingDir" );

    org.powermock.reflect.Whitebox.invokeMethod( jobExecutor, "setS3BucketKey", stagingFileObject );

    String bucketKeyWithFileName = (String) Whitebox.getInternalState( jobExecutor, "key" );

    assertEquals( expectedKey, bucketKeyWithFileName );
  }

  @Test
  public void testSetS3BucketKey_keyNull() throws Exception {

    String bucketKey = null;
    String expectedKey = stagingFile.getName();

    FileObject stagingFileObject = KettleVFS.getFileObject( stagingFile.getPath() );

    PowerMockito.doReturn( bucketKey ).when( jobExecutor, "getKeyFromS3StagingDir" );

    org.powermock.reflect.Whitebox.invokeMethod( jobExecutor, "setS3BucketKey", stagingFileObject );

    String bucketKeyWithFileName = (String) Whitebox.getInternalState( jobExecutor, "key" );

    assertEquals( expectedKey, bucketKeyWithFileName );
  }

  @Test
  public void testSetS3BucketKey_keyEmptyString() throws Exception {

    String bucketKey = "";
    String expectedKey = stagingFile.getName();

    FileObject stagingFileObject = KettleVFS.getFileObject( stagingFile.getPath() );

    PowerMockito.doReturn( bucketKey ).when( jobExecutor, "getKeyFromS3StagingDir" );

    org.powermock.reflect.Whitebox.invokeMethod( jobExecutor, "setS3BucketKey", stagingFileObject );

    String bucketKeyWithFileName = (String) Whitebox.getInternalState( jobExecutor, "key" );

    assertEquals( expectedKey, bucketKeyWithFileName );
  }

  @Test
  public void testGetStagingBucketName_OneFolder() throws Exception {

    String expectedBucketName = "test";

    PowerMockito.doReturn( "/test" ).when( jobExecutor, "getS3FileObjectPath" );

    String bucketName = jobExecutor.getStagingBucketName();

    assertEquals( expectedBucketName, bucketName );
  }

  @Test
  public void testGetStagingBucketName_withSubfolder() throws Exception {

    String expectedBucketName = "test";

    PowerMockito.doReturn( "/test/hive" ).when( jobExecutor, "getS3FileObjectPath" );

    String bucketName = jobExecutor.getStagingBucketName();

    assertEquals( expectedBucketName, bucketName );
  }
}
