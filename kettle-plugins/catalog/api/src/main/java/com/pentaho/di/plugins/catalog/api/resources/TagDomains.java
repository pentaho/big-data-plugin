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

package com.pentaho.di.plugins.catalog.api.resources;

import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.entities.metadata.TagDomain;
import com.pentaho.di.plugins.catalog.api.entities.metadata.TagResult;

import java.util.List;

public class TagDomains extends Resource {
  private static final String BASE_URL = CatalogClient.API_ROOT + "/tagdomain/all";
  private static final String READ_URL = BASE_URL + "/";

  public TagDomains( CatalogClient catalogClient ) {
    super( catalogClient );
  }

  public TagResult doTags() {
    TagResult tagResult = new TagResult();
    List<TagDomain> tagDomains = readAll( READ_URL, TagDomain.class );

    tagDomains.stream()
            .filter( td -> td.getTags() != null && !td.getTags().isEmpty() )
            .forEach( td -> td.getTags()
                    .stream()
                    .forEach( t -> tagResult.put( t.getName(), t.getKey() ) ) );

    return tagResult;
  }
}
