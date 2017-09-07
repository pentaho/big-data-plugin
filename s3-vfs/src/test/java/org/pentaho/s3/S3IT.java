/*!
* Copyright 2010 - 2017 Hitachi Vantara.  All rights reserved.
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

package org.pentaho.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import org.apache.http.impl.conn.tsccm.BasicPoolEntry;
import org.apache.http.impl.conn.tsccm.ConnPoolByRoute;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class S3IT {

  private static FileSystemManager fsManager;
  private static String HELLO_S3_STR = "Hello S3 VFS";
  public static String awsAccessKey;
  public static String awsSecretKey;
  public static S3Service service;
  public static boolean debug;

  private FileSelector deleteFileSelector = new FileSelector() {
    public boolean includeFile( FileSelectInfo arg0 ) throws Exception {
      return true;
    }

    public boolean traverseDescendents( FileSelectInfo arg0 ) throws Exception {
      return true;
    }
  };

  @BeforeClass
  public static void beforeClass() throws Exception {
    fsManager = VFS.getManager();
    debug = Boolean.parseBoolean( System.getProperty( "debug", "false" ) );
    if ( debug ) {
      System.out.println( "Debug mode is enabled" + System.getProperty( "debug" ) );
    }
    Properties settings = new Properties();
    settings.load( S3IT.class.getResourceAsStream( "/test-settings.properties" ) );
    awsAccessKey = settings.getProperty( "awsAccessKey" );
    awsSecretKey = settings.getProperty( "awsSecretKey" );

    AWSCredentials awsCredentials = new AWSCredentials( awsAccessKey, awsSecretKey );
    Jets3tProperties.getInstance( Constants.JETS3T_PROPERTIES_FILENAME )
      .setProperty( "httpclient.max-connections", "10" );
    service = new RestS3Service( awsCredentials );

    S3Bucket[] myBuckets = service.listAllBuckets();

    for ( S3Bucket bucket : myBuckets ) {
      try {
        System.out.println( bucket.getName() );
        if ( debug ) {
          S3Object[] objs = service.listObjects( bucket.getName() );
          for ( S3Object obj : objs ) {
            System.out.println( "\t" + obj.getKey() );
          }
        }
      } catch ( Throwable t ) {
        t.printStackTrace();
      }
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  public static String buildS3URL( String path ) throws UnsupportedEncodingException {
    return "s3://" + URLEncoder.encode( awsAccessKey, "UTF-8" ) + ":" + URLEncoder.encode( awsSecretKey, "UTF-8" )
      + "@S3" + path;
  }

  @Test
  public void readFile() throws Exception {
    assertNotNull( "FileSystemManager is null", fsManager );

    FileObject bucket = fsManager.resolveFile( buildS3URL( "/mdamour_read_file_bucket_test" ) );
    bucket.createFolder();

    FileObject s3FileOut = fsManager.resolveFile( buildS3URL( "/mdamour_read_file_bucket_test/writeFileTest" ) );
    OutputStream out = s3FileOut.getContent().getOutputStream();
    out.write( HELLO_S3_STR.getBytes( "UTF-8" ) );
    out.close();

    ByteArrayOutputStream testOut = new ByteArrayOutputStream();
    IOUtils.copy( s3FileOut.getContent().getInputStream(), testOut );
    assertEquals( HELLO_S3_STR.getBytes().length, testOut.toByteArray().length );
    assertEquals( new String( HELLO_S3_STR.getBytes( "UTF-8" ), "UTF-8" ),
      new String( testOut.toByteArray(), "UTF-8" ) );

    bucket.delete( deleteFileSelector );
    assertEquals( false, bucket.exists() );
    testConnectionsLeak();
  }

  @Test
  public void writeFile() throws Exception {
    assertNotNull( "FileSystemManager is null", fsManager );

    FileObject bucket = fsManager.resolveFile( buildS3URL( "/mdamour_write_file_bucket_test" ) );
    bucket.createFolder();

    FileObject s3FileOut = fsManager.resolveFile( buildS3URL( "/mdamour_write_file_bucket_test/writeFileTest" ) );
    OutputStream out = s3FileOut.getContent().getOutputStream();
    out.write( HELLO_S3_STR.getBytes() );
    out.close();

    IOUtils.copy( s3FileOut.getContent().getInputStream(), System.out );

    bucket.delete( deleteFileSelector );
    assertEquals( false, bucket.exists() );
    testConnectionsLeak();
  }

  @Test
  public void deleteFileAndFolder() throws Exception {
    assertNotNull( "FileSystemManager is null", fsManager );

    FileObject bucket = fsManager.resolveFile( buildS3URL( "/pentaho_pks_bucket_test" ) );
    bucket.createFolder();

    FileObject file1 = fsManager.resolveFile( buildS3URL( "/pentaho_pks_bucket_test/file1" ) );
    OutputStream out1 = file1.getContent().getOutputStream();
    out1.write( HELLO_S3_STR.getBytes() );
    out1.close();

    FileObject folder1 = fsManager.resolveFile( buildS3URL( "/pentaho_pks_bucket_test/folder1" ) );
    folder1.createFolder();

    FileObject s3FileOut2 = fsManager.resolveFile( buildS3URL( "/pentaho_pks_bucket_test/folder1/file2" ) );
    OutputStream out2 = s3FileOut2.getContent().getOutputStream();
    out2.write( HELLO_S3_STR.getBytes() );
    out2.close();

    FileObject folder2 = fsManager.resolveFile( buildS3URL( "/pentaho_pks_bucket_test/folder2" ) );
    folder2.createFolder();

    FileObject folder3 = fsManager.resolveFile( buildS3URL( "/pentaho_pks_bucket_test/folder1/folder3" ) );
    folder3.createFolder();

    file1.delete( deleteFileSelector );        // Delete a file.
    assertEquals( false, file1.exists() );

    folder1.delete( deleteFileSelector );      // Delete a non-empty folder.
    assertEquals( false, folder1.exists() );

    folder1.createFolder();
    folder1.delete( deleteFileSelector );      // Delete an empty folder.
    assertEquals( false, folder1.exists() );

    bucket.delete( deleteFileSelector );       // Delete a bucket
    assertEquals( false, bucket.exists() );
    testConnectionsLeak();
  }

  @Test
  public void createBucket() throws Exception {
    assertNotNull( "FileSystemManager is null", fsManager );
    FileObject bucket = fsManager.resolveFile( buildS3URL( "/mdamour_create_bucket_test" ) );
    bucket.createFolder();
    assertEquals( true, bucket.exists() );
    bucket.delete( deleteFileSelector );
    assertEquals( false, bucket.exists() );
    testConnectionsLeak();
  }

  @Test
  public void deleteBucket() throws Exception {
    assertNotNull( "FileSystemManager is null", fsManager );
    FileObject bucket = fsManager.resolveFile( buildS3URL( "/mdamour_delete_bucket_test" ) );
    assertEquals( false, bucket.exists() );
    bucket.createFolder();
    assertEquals( true, bucket.exists() );
    bucket.delete();
    assertEquals( false, bucket.exists() );
    testConnectionsLeak();
  }

  @Test
  public void lastModifiedDate() throws Exception {
    assertNotNull( "FileSystemManager is null", fsManager );

    FileObject bucket = fsManager.resolveFile( buildS3URL( "/mdamour_last_modified_bucket_test" ) );
    assertEquals( false, bucket.exists() );
    bucket.createFolder();
    assertEquals( true, bucket.exists() );
    assertTrue( bucket.getContent().getLastModifiedTime() == -1 );

    long before = System.currentTimeMillis();
    FileObject s3FileOut = fsManager.resolveFile( buildS3URL( "/mdamour_last_modified_bucket_test/file01" ) );
    OutputStream out = s3FileOut.getContent().getOutputStream();
    out.write( HELLO_S3_STR.getBytes() );
    out.close();

    long lastMod = s3FileOut.getContent().getLastModifiedTime();

    assertTrue( lastMod >= before );

    bucket.delete( deleteFileSelector );
    assertEquals( false, bucket.exists() );
    testConnectionsLeak();
  }

  @Test
  public void createDeleteRecursive() throws Exception {
    assertNotNull( "FileSystemManager is null", fsManager );

    FileObject bucket = fsManager.resolveFile( buildS3URL( "/mdamour_create_delete_test" ) );
    // assertEquals(false, bucket.exists());
    bucket.createFolder();
    assertEquals( true, bucket.exists() );

    FileObject s3FileOut = fsManager.resolveFile( buildS3URL( "/mdamour_create_delete_test/folder1/folder11" ) );
    s3FileOut.createFolder();
    assertEquals( true, s3FileOut.exists() );

    s3FileOut = fsManager.resolveFile( buildS3URL( "/mdamour_create_delete_test/folder1/child" ) );
    s3FileOut.createFile();
    OutputStream out = s3FileOut.getContent().getOutputStream();
    out.write( HELLO_S3_STR.getBytes() );
    out.close();

    s3FileOut = fsManager.resolveFile( buildS3URL( "/mdamour_create_delete_test/folder2/child" ) );
    s3FileOut.createFile();
    out = s3FileOut.getContent().getOutputStream();
    out.write( HELLO_S3_STR.getBytes() );
    out.close();

    bucket = fsManager.resolveFile( buildS3URL( "/mdamour_create_delete_test" ) );
    printFileObject( bucket, 0 );

    FileObject parentFolder1 = fsManager.resolveFile( buildS3URL( "/mdamour_create_delete_test/folder1" ) );
    parentFolder1.delete( deleteFileSelector );
    assertEquals( false, parentFolder1.exists() );

    bucket = fsManager.resolveFile( buildS3URL( "/mdamour_create_delete_test" ) );
    printFileObject( bucket, 0 );

    bucket.delete( deleteFileSelector );
    assertEquals( false, bucket.exists() );
    testConnectionsLeak();
  }


  @Test
  public void listChildren() throws Exception {
    assertNotNull( "FileSystemManager is null", fsManager );

    FileObject bucket = fsManager.resolveFile( buildS3URL( "/mdamour_list_children_test" ) );
    // assertEquals(false, bucket.exists());
    bucket.createFolder();
    assertEquals( true, bucket.exists() );

    FileObject s3FileOut = fsManager.resolveFile( buildS3URL( "/mdamour_list_children_test/child01" ) );
    s3FileOut.createFile();
    OutputStream out = s3FileOut.getContent().getOutputStream();
    out.write( HELLO_S3_STR.getBytes() );
    out.close();

    s3FileOut = fsManager.resolveFile( buildS3URL( "/mdamour_list_children_test/child02" ) );
    s3FileOut.createFile();
    out = s3FileOut.getContent().getOutputStream();
    out.write( HELLO_S3_STR.getBytes() );
    out.close();

    s3FileOut = fsManager.resolveFile( buildS3URL( "/mdamour_list_children_test/child03" ) );
    s3FileOut.createFile();
    out = s3FileOut.getContent().getOutputStream();
    out.write( HELLO_S3_STR.getBytes() );
    out.close();

    bucket = fsManager.resolveFile( buildS3URL( "/mdamour_list_children_test" ) );
    printFileObject( bucket, 0 );

    bucket.delete( deleteFileSelector );
    assertEquals( false, bucket.exists() );
    testConnectionsLeak();
  }

  @Test
  public void testDoGetType() throws Exception {
    assertNotNull( "FileSystemManager is null", fsManager );

    FileObject bucket = fsManager.resolveFile( buildS3URL( "/mdamour_get_type_test" ) );
    // assertEquals(false, bucket.exists());
    bucket.createFolder();
    assertEquals( true, bucket.exists() );

    FileObject s3FileOut = fsManager.resolveFile( buildS3URL( "/mdamour_get_type_test/child01" ) );
    s3FileOut.createFile();
    OutputStream out = s3FileOut.getContent().getOutputStream();
    out.write( HELLO_S3_STR.getBytes() );
    out.close();
    assertEquals( "Is not a folder type", FileType.FOLDER, bucket.getType() );
    assertEquals( "Is not a file type", FileType.FILE, s3FileOut.getType() );

    fsManager.closeFileSystem( bucket.getFileSystem() );
    assertEquals( "Is not a folder type (after clearing cache)", FileType.FOLDER,
      fsManager.resolveFile( buildS3URL( "/mdamour_get_type_test" ) ).getType() );
    assertEquals( "Is not a file type (after clearing cache)", FileType.FILE,
      fsManager.resolveFile( buildS3URL( "/mdamour_get_type_test/child01" ) ).getType() );

    bucket = fsManager.resolveFile( buildS3URL( "/mdamour_get_type_test" ) );
    printFileObject( bucket, 0 );

    bucket.delete( deleteFileSelector );
    assertEquals( false, bucket.exists() );
    testConnectionsLeak();
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void testConnectionsLeak() throws Exception {
    Field fPool = ThreadSafeClientConnManager.class.getDeclaredField( "pool" );
    Object ob = service.getHttpConnectionManager();
    fPool.setAccessible( true );
    ConnPoolByRoute pool = (ConnPoolByRoute) fPool.get( ob );
    fPool.setAccessible( false );

    Field fLeasedConnections = ConnPoolByRoute.class.getDeclaredField( "leasedConnections" );
    fLeasedConnections.setAccessible( true );
    Set<BasicPoolEntry> set = (Set<BasicPoolEntry>) fLeasedConnections.get( pool );
    fLeasedConnections.setAccessible( false );

    int connections = pool.getConnectionsInPool();

    assertTrue(
      "Http connections leak was found. Check either unclosed streams in test methods or incorrect using S3FileObject"
        + ".getS3Object. Pool size is 10. Connections in pool=" + connections + "leasedConnections=" + set.size(),
      set.size() == 0 );
  }

  private void printFileObject( FileObject fileObject, int depth ) throws Exception {
    if ( debug ) {
      for ( int i = 0; i < depth; i++ ) {
        System.out.print( "    " );
      }
      System.out.println( fileObject.getName().getBaseName() );

      if ( fileObject.getType() == FileType.FOLDER ) {
        FileObject[] children = fileObject.getChildren();
        for ( FileObject child : children ) {
          printFileObject( child, depth + 1 );
        }
      }
    }
  }

}
