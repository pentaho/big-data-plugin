/*
 * ! ******************************************************************************
 *  *
 *  * Pentaho Data Integration
 *  *
 *  * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *  *
 *  *******************************************************************************
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with
 *  * the License. You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  *
 *  *****************************************************************************
 */

package org.pentaho.amazon.s3;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class S3FileOutputMetaProcessFilenameTest {

  private S3FileOutputMeta meta = null;

  @Before
  public void beforeTest() {
    meta = new S3FileOutputMeta();
    meta.setAccessKey( "" );
    meta.setSecretKey( "" );
    meta.setFileName( "" );
  }

  @Test
  public void testProcessFilenameOldStyleNotEncoded() throws Exception {
    String name = "s3://AAAAAAABBBBBBBBB333A:Qqqqqqqqqqq+uP777777/RRRRRRRRRRRR+dWvhzS@s3/dbahdano/empty";
    meta.processFilename( name );
    check( "AAAAAAABBBBBBBBB333A", "Qqqqqqqqqqq+uP777777/RRRRRRRRRRRR+dWvhzS", "s3://s3/dbahdano/empty" );
  }

  @Test
  public void testProcessFilenameCapitalLetter() throws Exception {
    String name = "S3://AAAAAAABBBBBBBBB333A:Qqqqqqqqqqq+uP777777/RRRRRRRRRRRR+dWvhzS@s3/dbahdano/empty";
    meta.processFilename( name );
    check( "AAAAAAABBBBBBBBB333A", "Qqqqqqqqqqq+uP777777/RRRRRRRRRRRR+dWvhzS", "s3://s3/dbahdano/empty" );
  }

  @Test
  public void testProcessFilenameOldStyleEncoded() throws Exception {
    String name = "s3://AAAAAAABBBBBBBBB333A:Q123456789%2BqwertyUIO%2FREFfJUW7FdNY%2BdWvhzS@s3/dbahdano/empty";
    meta.processFilename( name );
    check( "AAAAAAABBBBBBBBB333A", "Q123456789+qwertyUIO/REFfJUW7FdNY+dWvhzS", "s3://s3/dbahdano/empty" );
  }

  @Test
  public void testProcessFilenameNewStyle1() throws Exception {
    String name = "s3://s3/dbahdano/empty";
    meta.processFilename( name );
    check( "", "", "s3://s3/dbahdano/empty" );
  }

  @Test
  public void testProcessFilenameNewStyle2() throws Exception {
    String name = "s3://s/dbahdano/empty";
    meta.processFilename( name );
    check( "", "", "s3://s/dbahdano/empty" );
  }

  private void check( String expectedAccessKey, String expectedSecretKey, String expectedFilename ) throws Exception {
    assertEquals( "Access keys are not equal", expectedAccessKey, meta.getAccessKey() );
    assertEquals( "Secret keys are not equal", expectedSecretKey, meta.getSecretKey() );
    assertEquals( "File names are not equal", expectedFilename, meta.getFileName() );

  }
}
