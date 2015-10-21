/*!
* Copyright 2010 - 2015 Pentaho Corporation.  All rights reserved.
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

import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticator;
import org.jets3t.service.security.AWSCredentials;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class S3FileUtilTest {

  @Before
  public void setUp() throws Exception {

  }

  @Test
  public void testResolveFileUriAuthenticator() throws Exception {
    String url = "ram:///myfile.txt";
    UserAuthenticator userAuthenticator = mock( UserAuthenticator.class );
    assertNotNull( S3FileUtil.resolveFile( url, userAuthenticator ) );
  }

  @Test
  public void testResolveFileUriUserPass() throws Exception {
    assertNotNull( S3FileUtil.resolveFile( "ram:///myfile.txt", "username", "password" ) );
  }

  @Test
  public void testResolveFileUriCredentials() throws Exception {
    String url = "ram:///myfile.txt";
    AWSCredentials credentials = mock( AWSCredentials.class );
    assertNotNull( S3FileUtil.resolveFile( url, credentials ) );
  }

  @Test
  public void testResolveFileUriOptions() throws Exception {
    String url = "ram:///myfile.txt";
    FileSystemOptions options = new FileSystemOptions();
    assertNotNull( S3FileUtil.resolveFile( url, options ) );
  }

  @Test
  public void testDefaultConstructor() {
    // Because we don't privatize the default constructor, we need to test it. This test will no longer compile
    // if/when the default constructor is made private.
    assertNotNull( new S3FileUtil() );
  }
}
