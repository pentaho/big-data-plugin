/*!
 * Copyright 2020 - 2021 Hitachi Vantara.  All rights reserved.
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
import static org.junit.Assert.assertNull;

public class S3UtilTest {

  @Test
  public void getKeysFromURITest() {
    S3Util.S3Keys keys = S3Util.getKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y@s3n/mybucket/something" );
    assertEquals( "ThiSiSA+PossibleAcce/ssK3y", keys.getAccessKey() );
    assertEquals( "PossiblES3cre+K3y", keys.getSecretKey() );

    keys = S3Util.getKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y@s3n" );
    assertEquals( "ThiSiSA+PossibleAcce/ssK3y", keys.getAccessKey() );
    assertEquals( "PossiblES3cre+K3y", keys.getSecretKey() );

    keys = S3Util.getKeysFromURI( "s3://ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y@s3" );
    assertEquals( "ThiSiSA+PossibleAcce/ssK3y", keys.getAccessKey() );
    assertEquals( "PossiblES3cre+K3y", keys.getSecretKey() );

    keys = S3Util.getKeysFromURI( "s3a://ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y@s3a" );
    assertEquals( "ThiSiSA+PossibleAcce/ssK3y", keys.getAccessKey() );
    assertEquals( "PossiblES3cre+K3y", keys.getSecretKey() );
  }

  @Test
  public void getKeysFromURIWithoutKeysTest() {
    S3Util.S3Keys keys = S3Util.getKeysFromURI( "s3n://s3n/mybucket/something" );
    assertNull( keys );
  }

  @Test
  public void getKeysFromURIWrongKeyTest() {
    S3Util.S3Keys keys = S3Util.getKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3yPossiblES3cre+K3y@s3n/mybucket/something" );
    assertNull( keys );
  }

  @Test
  public void getKeysFromNameParserURITest() {
    String keys = S3Util.getFullKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y@s3n/mybucket/something" );
    assertEquals( "ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y@", keys );
  }

  @Test
  public void getKeysFromNameParserURIWrongKeyTest() {
    String keys = S3Util.getFullKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3yPossiblES3cre+K3y@s3n/mybucket/something" );
    assertNull( keys );
  }

  @Test
  public void getKeysFromNameParserURIWithoutKeysTest() {
    String keys = S3Util.getFullKeysFromURI( "s3n://s3n/mybucket/something" );
    assertNull( keys );
  }

  @Test
  public void getMultipleKeys() {
    S3Util.S3Keys k1 = S3Util.getKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3y:PossiblES3cre+K3y@s3n/mybucket/something" );
    S3Util.S3Keys k2 = S3Util.getKeysFromURI( "s3n://ThiSiSA+PossibleAcce/ssK3yPossib:lES3cre+K3y@s3n/mybucket/something" );
    assertEquals( "ThiSiSA+PossibleAcce/ssK3y", k1.getAccessKey() );
    assertEquals( "ThiSiSA+PossibleAcce/ssK3yPossib", k2.getAccessKey() );
    assertEquals( "PossiblES3cre+K3y", k1.getSecretKey() );
    assertEquals( "lES3cre+K3y", k2.getSecretKey() );
  }
}
