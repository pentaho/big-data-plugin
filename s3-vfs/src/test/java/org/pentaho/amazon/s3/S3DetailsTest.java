/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon.s3;

import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3DetailsTest {

  private S3Details s3Details = new S3Details();

  @Test
  public void getProperties() {
    s3Details.setName( "name" );
    s3Details.setRegion( "aws-west-1" );
    s3Details.setAccessKey( "ASIAXJ3TZZPFVO3NK6O" );
    s3Details.setSecretKey( "jKbmptEdHk6cTXXqGodacxJn5yaETIIhKjJb/oZ" );

    Map<String, String> props = s3Details.getProperties();
    assertThat( props.get( "name" ), equalTo( "name" ) );
    assertThat( props.get( "region" ), equalTo( "aws-west-1" ) );
    assertThat( props.get( "accessKey" ), equalTo( "ASIAXJ3TZZPFVO3NK6O" ) );
    assertThat( props.get( "secretKey" ), equalTo( "jKbmptEdHk6cTXXqGodacxJn5yaETIIhKjJb/oZ" ) );
    assertThat( props.size(), equalTo( 15 ) );
    assertThat( props.entrySet().stream().filter( e -> e.getValue() != null ).count(), equalTo( 4L ) );
  }

  @Test
  public void getPropertiesWithEndpoint() {
    s3Details.setName( "name" );
    s3Details.setRegion( "aws-west-1" );
    s3Details.setAccessKey( "ASIAXJ3TZZPFVO3NK6O" );
    s3Details.setSecretKey( "jKbmptEdHk6cTXXqGodacxJn5yaETIIhKjJb/oZ" );
    s3Details.setEndpoint( "http://localhost:9000" );
    s3Details.setPathStyleAccess( "true");
    s3Details.setSignatureVersion( "v4" );

    Map<String, String> props = s3Details.getProperties();
    assertThat( props.get( "name" ), equalTo( "name" ) );
    assertThat( props.get( "region" ), equalTo( "aws-west-1" ) );
    assertThat( props.get( "accessKey" ), equalTo( "ASIAXJ3TZZPFVO3NK6O" ) );
    assertThat( props.get( "secretKey" ), equalTo( "jKbmptEdHk6cTXXqGodacxJn5yaETIIhKjJb/oZ" ) );
    assertThat( props.get( "endpoint" ), equalTo( "http://localhost:9000" ) );
    assertThat( props.get( "pathStyleAccess" ), equalTo( "true" ) );
    assertThat( props.get( "signatureVersion" ), equalTo( "v4" ) );

    assertThat( props.size(), equalTo( 15 ) );
    assertThat( props.entrySet().stream().filter( e -> e.getValue() != null ).count(), equalTo( 7L ) );
  }

}
