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

package org.pentaho.big.data.it;

/**
 * How a transformation is executed against the PDI container.
 * Only {@link #PAN} is supported for now; CARTE/SERVER can be added later.
 */
public enum ExecutionType {
  PAN
}
