/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.authentication.mapper.api;

/**
 * @author bryan
 */
public interface AuthenticationMappingManager {
  String RANKING_CONFIG = "service.ranking";

  <InputType, OutputType> OutputType getMapping( Class<InputType> inputType, InputType input,
                                                 Class<OutputType> outputType ) throws MappingException;
}
