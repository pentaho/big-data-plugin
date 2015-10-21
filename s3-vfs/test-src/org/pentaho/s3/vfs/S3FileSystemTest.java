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

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for S3FileSystem
 */
public class S3FileSystemTest {

  S3FileSystem fileSystem;
  S3FileName fileName;

  @Before
  public void setUp() throws Exception {
    fileName = new S3FileName(
      S3FileNameTest.SCHEME,
      S3FileNameTest.HOST,
      S3FileNameTest.PORT,
      S3FileNameTest.PORT,
      S3FileNameTest.awsAccessKey,
      S3FileNameTest.awsSecretKey,
      "/",
      FileType.FOLDER,
      null );
    fileSystem = new S3FileSystem( fileName, new FileSystemOptions() );
  }

  @Test
  public void testAddCapabilities() throws Exception {
    Collection<Capability> capabilities = new ArrayList<>();
    fileSystem.addCapabilities( capabilities );
    assertTrue( capabilities.contains( Capability.CREATE ) );
    assertTrue( capabilities.contains( Capability.DELETE ) );
    assertTrue( capabilities.contains( Capability.RENAME ) );
    assertTrue( capabilities.contains( Capability.GET_TYPE ) );
    assertTrue( capabilities.contains( Capability.LIST_CHILDREN ) );
    assertTrue( capabilities.contains( Capability.READ_CONTENT ) );
    assertTrue( capabilities.contains( Capability.URI ) );
    assertTrue( capabilities.contains( Capability.WRITE_CONTENT ) );
    assertTrue( capabilities.contains( Capability.GET_LAST_MODIFIED ) );
    assertTrue( capabilities.contains( Capability.RANDOM_ACCESS_READ ) );
    assertFalse( capabilities.contains( Capability.RANDOM_ACCESS_WRITE ) );

  }

  @Test
  public void testCreateFile() throws Exception {
    AbstractFileName fileName = mock( AbstractFileName.class );
    assertNotNull( fileSystem.createFile( fileName ) );
  }

  @Test
  public void testGetS3Service() throws Exception {
    assertNotNull( fileSystem.getS3Service() );

    FileSystemOptions options = new FileSystemOptions();
    UserAuthenticator authenticator = mock( UserAuthenticator.class );
    UserAuthenticationData authData = mock( UserAuthenticationData.class );
    when( authenticator.requestAuthentication( S3FileProvider.AUTHENTICATOR_TYPES ) ).thenReturn( authData );
    when( authData.getData( UserAuthenticationData.USERNAME ) ).thenReturn( "username".toCharArray() );
    when( authData.getData( UserAuthenticationData.PASSWORD ) ).thenReturn( "password".toCharArray() );
    DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( options, authenticator );

    fileSystem = new S3FileSystem( fileName, options );
    assertNotNull( fileSystem.getS3Service() );
  }
}
