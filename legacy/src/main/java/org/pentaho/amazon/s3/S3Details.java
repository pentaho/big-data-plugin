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

import com.amazonaws.regions.Regions;
import org.pentaho.di.connections.annotations.Encrypted;
import org.pentaho.di.connections.vfs.VFSConnectionDetails;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;
import org.pentaho.s3n.vfs.S3NFileProvider;

import java.util.ArrayList;
import java.util.List;

@MetaStoreElementType(
  name = "Amazon S3 Connection",
  description = "Defines the connection details for an Amazon S3 connection" )
public class S3Details implements VFSConnectionDetails {

  @MetaStoreAttribute
  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  @Encrypted
  private String accessKey;

  @MetaStoreAttribute
  @Encrypted
  private String secretKey;

  @MetaStoreAttribute
  @Encrypted
  private String sessionToken;

  @MetaStoreAttribute
  private String credentialsFilePath;

  @MetaStoreAttribute
  @Encrypted
  private String credentialsFile;

  @MetaStoreAttribute
  private String authType;

  @MetaStoreAttribute
  private String region;

  @MetaStoreAttribute
  private String profileName;

  @Override public String getName() {
    return name;
  }

  @Override public void setName( String name ) {
    this.name = name;
  }

  @Override public String getType() {
    return S3NFileProvider.SCHEME;
  }

  @Override public String getDescription() {
    return description;
  }

  public void setDescription( String description ) {
    this.description = description;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey( String accessKey ) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey( String secretKey ) {
    this.secretKey = secretKey;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  public void setSessionToken( String sessionToken ) {
    this.sessionToken = sessionToken;
  }

  public String getCredentialsFilePath() {
    return credentialsFilePath;
  }

  public void setCredentialsFilePath( String credentialsFilePath ) {
    this.credentialsFilePath = credentialsFilePath;
  }

  public String getCredentialsFile() {
    return credentialsFile;
  }

  public void setCredentialsFile( String credentialsFile ) {
    this.credentialsFile = credentialsFile;
  }

  public String getAuthType() {
    return authType;
  }

  public void setAuthType( String authType ) {
    this.authType = authType;
  }

  public List<String> getRegions() {
    List<String> names = new ArrayList<>();
    for ( Regions region : Regions.values() ) {
      names.add( region.getName() );
    }
    return names;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion( String region ) {
    this.region = region;
  }

  public String getProfileName() {
    return profileName;
  }

  public void setProfileName( String profileName ) {
    this.profileName = profileName;
  }
}
