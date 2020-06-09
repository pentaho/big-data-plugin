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

package com.pentaho.di.plugins.catalog.api.entities.search;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties( ignoreUnknown = true )
public class FacetSelection {
  private String facetName;
  private String displayName;
  private List<String> facetCategories;

  public FacetSelection() {
    this.facetName = "";
    this.displayName = "";
    this.facetCategories = new ArrayList<>();
  }

  public FacetSelection( String facetName, String displayName, List<String> facetCategories ) {
    this.facetName = facetName;
    this.displayName = displayName;
    this.facetCategories = facetCategories;
  }

  public String getFacetName() {
    return facetName;
  }

  public void setFacetName( String facetName ) {
    this.facetName = facetName;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName( String displayName ) {
    this.displayName = displayName;
  }

  public List<String> getFacetCategories() {
    return facetCategories;
  }

  public void setFacetCategories( List<String> facetCategories ) {
    this.facetCategories = facetCategories;
  }
}

