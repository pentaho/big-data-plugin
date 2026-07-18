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


package org.pentaho.authentication.mapper.api;

import java.util.Map;

/**
 * @author bryan
 */
public interface AuthenticationMappingService<InputType, OutputType> {
  String getId();

  Class<? extends InputType> getInputType();

  Class<? extends OutputType> getOutputType();

  boolean accepts( Object input );

  OutputType getMapping( InputType input, Map<String, ?> config ) throws MappingException;
}
