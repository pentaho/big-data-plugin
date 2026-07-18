/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.amazon.client.api;

import com.amazonaws.services.pricing.model.AWSPricingException;

import java.io.IOException;
import java.util.List;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public interface PricingClient {

  List<String> populateInstanceTypesForSelectedRegion() throws AWSPricingException, IOException;
}
