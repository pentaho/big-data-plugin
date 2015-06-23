/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.s3.vfs;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.jets3t.service.security.AWSCredentials;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * created by: rfellows date:       5/21/12
 */
public class S3FileUtilTestIntegration {

  public static String awsAccessKey;
  public static String awsSecretKey;

  @BeforeClass
  public static void init() throws Exception {
    Properties settings = new Properties();
    settings.load( S3FileUtilTestIntegration.class.getResourceAsStream( "/test-settings.properties" ) );
    awsAccessKey = settings.getProperty( "awsAccessKey" );
    awsSecretKey = settings.getProperty( "awsSecretKey" );
  }

  @Test
  public void testResolveFile_userAutheticator() throws FileSystemException {
    UserAuthenticator userAuthenticator = new StaticUserAuthenticator( null, awsAccessKey, awsAccessKey );
    FileObject file = S3FileUtil.resolveFile( "s3://s3/", userAuthenticator );
    assertTrue( file.exists() );

    file = S3FileUtil.resolveFile( "s3://s3/_this_does_not_exist_", userAuthenticator );
    assertFalse( file.exists() );
  }

  @Test
  public void testResolveFile_username_pass() throws FileSystemException {
    FileObject file = S3FileUtil.resolveFile( "s3://s3/", awsAccessKey, awsSecretKey );
    assertTrue( file.exists() );

    file = S3FileUtil.resolveFile( "s3://s3/_this_does_not_exist_", awsAccessKey, awsSecretKey );
    assertFalse( file.exists() );
  }

  @Test
  public void testResolveFile_AwsCredentials() throws FileSystemException {
    AWSCredentials credentials = new AWSCredentials( awsAccessKey, awsSecretKey );
    FileObject file = S3FileUtil.resolveFile( "s3://s3/", credentials );
    assertTrue( file.exists() );

    file = S3FileUtil.resolveFile( "s3://s3/_this_does_not_exist_", credentials );
    assertFalse( file.exists() );
  }

}
