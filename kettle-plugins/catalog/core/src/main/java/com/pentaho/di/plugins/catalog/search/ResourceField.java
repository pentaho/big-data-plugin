/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2020 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */

package com.pentaho.di.plugins.catalog.search;

import org.pentaho.di.core.injection.Injection;

public class ResourceField {

  @Injection( name = "RESOURCE_FIELD_NAME", group = "RESOURCE_FIELDS" )
  private String name;

  @Injection( name = "RESOURCE_FIELD_ID", group = "RESOURCE_FIELDS" )
  private String id;

  @Injection( name = "RESOURCE_FIELD_TYPE", group = "RESOURCE_FIELDS" )
  private String type;

  @Injection( name = "RESOURCE_FIELD_ORIGIN", group = "RESOURCE_FIELDS" )
  private String origin;


  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getId() {
    return id;
  }

  public void setId( String id ) {
    this.id = id;
  }

  public String getType() {
    return type;
  }

  public void setType( String type ) {
    this.type = type;
  }

  public String getOrigin() {
    return origin;
  }

  public void setOrigin( String origin ) {
    this.origin = origin;
  }

  @Override
  @SuppressWarnings( "squid:S2975" )
  public Object clone() {
    try {
      return super.clone();
    } catch ( CloneNotSupportedException e ) {
      return new Object();
    }
  }
}
