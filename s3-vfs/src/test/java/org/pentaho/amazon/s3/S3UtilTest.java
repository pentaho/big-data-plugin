/*!
 * Copyright 2020 - 2020 Hitachi Vantara.  All rights reserved.
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
package org.pentaho.amazon.s3;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S3UtilTest {

  @Test
  public void getKeysFromURITest() {
    String keys = S3Util.getKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y@s3n/mybucket/something",
      S3Util.URI_AWS_KEYS_GROUP );
    assertEquals( "ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y", keys );
  }

  @Test
  public void getKeysFromURIWithoutKeysTest() {
    String keys = S3Util.getKeysFromURI( "s3n://s3n/mybucket/something",
      S3Util.URI_AWS_KEYS_GROUP );
    assertEquals( "", keys );
  }

  @Test
  public void getKeysFromURIWrongKeyTest() {
    String keys = S3Util.getKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3yPossiblES3cre+K3y@s3n/mybucket/something",
      S3Util.URI_AWS_KEYS_GROUP );
    assertEquals( "", keys );
  }

  @Test
  public void getKeysFromNameParserURITest() {
    String keys = S3Util.getKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y@s3n/mybucket/something",
      S3Util.URI_AWS_FULL_KEYS_GROUP );
    assertEquals( "ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y@", keys );
  }

  @Test
  public void getKeysFromNameParserURIWrongKeyTest() {
    String keys = S3Util.getKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3yPossiblES3cre+K3y@s3n/mybucket/something",
      S3Util.URI_AWS_FULL_KEYS_GROUP );
    assertEquals( "", keys );
  }

  @Test
  public void getKeysFromNameParserURIWithoutKeysTest() {
    String keys = S3Util.getKeysFromURI( "s3n://s3n/mybucket/something",
      S3Util.URI_AWS_FULL_KEYS_GROUP );
    assertEquals( "", keys );
  }
}
