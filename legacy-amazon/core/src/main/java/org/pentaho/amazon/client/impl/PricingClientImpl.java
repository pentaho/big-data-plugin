/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon.client.impl;

import com.amazonaws.services.pricing.AWSPricing;
import com.amazonaws.services.pricing.model.AWSPricingException;
import com.amazonaws.services.pricing.model.Filter;
import com.amazonaws.services.pricing.model.GetProductsRequest;
import com.amazonaws.services.pricing.model.GetProductsResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.pentaho.amazon.InstanceType;
import org.pentaho.amazon.client.api.PricingClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public class PricingClientImpl implements PricingClient {

  private AWSPricing pricing;
  private String humanReadableRegion;
  private Collection<Filter> filters = new ArrayList<>();
  private List<String> instanceTypes;

  private static final String FIELD_TYPE = "TERM_MATCH";

  public PricingClientImpl( AWSPricing pricing, String humanReadableRegion ) {
    this.pricing = pricing;
    this.humanReadableRegion = humanReadableRegion;
  }

  private static Filter createProductFilter( String fieldType, String fieldName, String fieldValue ) {
    Filter fieldFilter = new Filter();
    fieldFilter.setType( fieldType );
    fieldFilter.setField( fieldName );
    fieldFilter.setValue( fieldValue );

    return fieldFilter;
  }

  private void addFiltersToProductRequest() {
    filters.add( createProductFilter( FIELD_TYPE, "softwareType", "EMR" ) );
    filters.add( createProductFilter( FIELD_TYPE, "location", humanReadableRegion ) );
  }

  private GetProductsRequest initProductsRequest() {
    GetProductsRequest productsRequest = new GetProductsRequest();
    addFiltersToProductRequest();
    productsRequest.setServiceCode( "ElasticMapReduce" );
    productsRequest.setFilters( filters );

    return productsRequest;
  }

  @VisibleForTesting
  protected List<String> getProductDescriptions() {
    GetProductsRequest productsRequest = initProductsRequest();
    GetProductsResult productsResult = pricing.getProducts( productsRequest );
    List<String> productDescriptions = productsResult.getPriceList();

    return productDescriptions;
  }

  @Override
  public List<String> populateInstanceTypesForSelectedRegion() throws AWSPricingException, IOException {

    List<String> productDescriptions = getProductDescriptions();

    if ( productDescriptions == null || productDescriptions.size() == 0 ) {
      return instanceTypes;
    }

    List<InstanceType> tmpInstanceTypes = new ArrayList<>();

    ObjectMapper mapper = new ObjectMapper();
    String instanceTypeName;
    String instanceFamily;

    for ( String description : productDescriptions ) {
      instanceTypeName =
        mapper.readTree( description ).path( "product" ).get( "attributes" ).get( "instanceType" ).asText();
      instanceFamily =
        mapper.readTree( description ).path( "product" ).get( "attributes" ).get( "instanceFamily" ).asText();
      tmpInstanceTypes.add( new InstanceType( instanceTypeName, instanceFamily ) );
    }

    instanceTypes = InstanceType.sortInstanceTypes( tmpInstanceTypes );

    return instanceTypes;
  }
}
