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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class FacetsResult extends HashMap<String, List<FacetSelection>> {
  public static final String DATA_RESOURCE = "data_resource";

  public List<FacetSelection> getFacetSelections( Facet facetEnum ) {
    return this.get( facetEnum.getName() );
  }

  public List<String> getFacetSelection( Facet facetEnum ) {
    List<String> result = new ArrayList<>();

    // The file size facets are not returned in the results, hard code them for now.
    if ( facetEnum.equals( Facet.FILE_SIZE ) ) {
      result = new ArrayList<>( Arrays.asList( "Below 10K", "Below 20K", "Over 20K" ) );
    }

    List<FacetSelection> facetSelections = this.get( DATA_RESOURCE );
    if ( facetSelections != null ) {
      for ( FacetSelection fs : facetSelections ) {
        if ( facetEnum.getName().equals( fs.getFacetName() ) ) {
          result = fs.getFacetCategories();
          break;
        }
      }
    }

    return result;
  }
}
