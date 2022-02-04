/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2022 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.pig;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.filter.AbstractFilter;

public class KettleLogChannelFilter extends AbstractFilter {
  String logChannelId;

  public KettleLogChannelFilter( String logChannelId ) {
    this.logChannelId = logChannelId;
  }

  @Override
  public Result filter( LogEvent event ) {
    if ( logChannelId.equals( event.getContextData().getValue( "logChannelId" ) ) ) {
      return Result.NEUTRAL;
    }
    return Result.DENY;
  }
}