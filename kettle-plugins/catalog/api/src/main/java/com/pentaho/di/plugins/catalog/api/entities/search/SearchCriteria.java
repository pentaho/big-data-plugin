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
import org.pentaho.database.util.Const;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@JsonIgnoreProperties( ignoreUnknown = true )
public class SearchCriteria {

  private String searchPhrase;
  private List<FacetSelection> facetSelections;
  private PagingCriteria pagingCriteria;
  private List<SortBySpecs> sortBySpecs;
  private List<String> entityScope;
  private String searchType = "ADVANCED";
  private Boolean preformedQuery = false;
  private String lastSelectedFacetName;
  private Boolean applyFacets = true;

  public SearchCriteria( SearchCriteriaBuilder builder ) {
    this.searchPhrase = builder.searchPhrase;
    this.facetSelections = builder.facetSelections;
    this.pagingCriteria = builder.pagingCriteria;
    this.sortBySpecs = builder.sortBySpecs;
    this.entityScope = builder.entityScope;
    this.searchType = builder.searchType;
    this.preformedQuery = builder.preformedQuery;
    this.lastSelectedFacetName = builder.lastSelectedFacetName;
    this.applyFacets = builder.applyFacets;
  }

  public String getSearchPhrase() {
    return searchPhrase;
  }

  public List<FacetSelection> getFacetSelections() {
    return facetSelections;
  }

  public PagingCriteria getPagingCriteria() {
    return pagingCriteria;
  }

  public List<SortBySpecs> getSortBySpecs() {
    return sortBySpecs;
  }

  public List<String> getEntityScope() {
    return entityScope;
  }

  public String getSearchType() {
    return searchType;
  }

  public Boolean getPreformedQuery() {
    return preformedQuery;
  }

  public String getLastSelectedFacetName() {
    return lastSelectedFacetName;
  }

  public Boolean getApplyFacets() {
    return applyFacets;
  }

  public static class SearchCriteriaBuilder {

    private static final String SEPARATOR = ",";

    private String searchPhrase = "";
    private List<FacetSelection> facetSelections = new ArrayList<>();
    private PagingCriteria pagingCriteria;
    private List<SortBySpecs> sortBySpecs = new ArrayList<>();
    private List<String> entityScope = new ArrayList<>();
    private String searchType = "ADVANCED";
    private Boolean preformedQuery = false;
    private String lastSelectedFacetName = "";
    private Boolean applyFacets = false;

    public SearchCriteriaBuilder searchPhrase( String searchPhrase ) {
      this.searchPhrase = searchPhrase;
      return this;
    }

    public SearchCriteriaBuilder facetSelections( List<FacetSelection> facetSelections ) {
      this.facetSelections = facetSelections;
      return this;
    }

    public SearchCriteriaBuilder addFacet( Facet facet, String value ) {
      if ( !Const.isEmpty( value ) ) {
        String[] values = value.split( SEPARATOR );
        facetSelections.add( new FacetSelection( facet.getName(), facet.getDisplayName(), Arrays.asList( values ) ) );
      }
      return this;
    }

    public SearchCriteriaBuilder pagingCriteria( PagingCriteria pagingCriteria ) {
      this.pagingCriteria = pagingCriteria;
      return this;
    }

    public SearchCriteriaBuilder sortBySpecs( List<SortBySpecs> sortBySpecs ) {
      this.sortBySpecs = sortBySpecs;
      return this;
    }

    public SearchCriteriaBuilder entityScope( List<String> entityScope ) {
      this.entityScope = entityScope;
      return this;
    }

    public SearchCriteriaBuilder searchType( String searchType ) {
      this.searchType = searchType;
      return this;
    }

    public SearchCriteriaBuilder preformedQuery( Boolean preformedQuery ) {
      this.preformedQuery = preformedQuery;
      return this;
    }

    public SearchCriteriaBuilder lastSelectedFacetName( String lastSelectedFacetName ) {
      this.lastSelectedFacetName = lastSelectedFacetName;
      return this;
    }

    public SearchCriteriaBuilder applyFacets( Boolean applyFacets ) {
      this.applyFacets = applyFacets;
      return this;
    }

    public SearchCriteria build() {
      return new SearchCriteria( this );
    }
  }
}

