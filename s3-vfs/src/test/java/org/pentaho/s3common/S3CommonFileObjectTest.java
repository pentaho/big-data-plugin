/*!
 * Copyright 2019 Hitachi Vantara.  All rights reserved.
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

package org.pentaho.s3common;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.Test;
import org.pentaho.s3n.vfs.S3NFileObject;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.reflect.Whitebox.getInternalState;
import static org.powermock.reflect.Whitebox.setInternalState;

public class S3CommonFileObjectTest {

  /**
   * Make sure the existing S3Object is closed before a new one is created.
   * Confirm that the new object is actually new and not the existing one.
   *
   * @throws IOException
   */
  @Test
  public void activateContentWithS3Object() throws IOException {
    S3CommonFileObject s3n = mock( S3CommonFileObject.class );
    doReturn( mock( S3Object.class ) ).when( s3n ).getS3Object();
    doCallRealMethod().when( s3n ).activateContent();

    S3Object s3object = mock( S3Object.class );
    setInternalState( s3n, "s3Object", s3object );

    s3n.activateContent();

    verify( s3object, times( 1 ) ).close();
    assertNotEquals( s3object, (S3Object) getInternalState( s3n, "s3Object" ) );
  }

  @Test
  public void activateContentWithNull() throws IOException {
    S3CommonFileObject s3n = mock( S3CommonFileObject.class );
    doReturn( mock( S3Object.class ) ).when( s3n ).getS3Object();
    doCallRealMethod().when( s3n ).activateContent();

    s3n.activateContent();

    assertNotNull( getInternalState( s3n, "s3Object" ) );
  }

  @Test
  public void getContentSize() {
    ObjectMetadata meta = mock( ObjectMetadata.class );
    doReturn( 1L ).when( meta ).getContentLength();

    S3Object s3object = mock( S3Object.class );
    doReturn( meta ).when( s3object ).getObjectMetadata();

    S3NFileObject s3n = mock( S3NFileObject.class );
    doReturn( s3object ).when( s3n ).getS3Object();
    doCallRealMethod().when( s3n ).doGetContentSize();

    assertEquals( 1L, s3n.doGetContentSize() );
  }
}
