/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.runtime.test.impl;

import org.pentaho.runtime.test.RuntimeTest;

import java.util.Comparator;
import java.util.Map;

/**
 * Created by bryan on 8/18/15.
 */
public class RuntimeTestComparator implements Comparator<RuntimeTest> {
  private final Map<String, Integer> orderedModules;

  public RuntimeTestComparator( Map<String, Integer> orderedModules ) {
    this.orderedModules = orderedModules;
  }

  private Integer nullSafeCompare( Object first, Object second ) {
    if ( first == null ) {
      if ( second == null ) {
        return null;
      } else {
        return 1;
      }
    }
    if ( second == null ) {
      return -1;
    }
    if ( first.equals( second ) ) {
      return 0;
    }
    return null;
  }

  private int compareModuleNames( String o1Module, String o2Module ) {
    Integer result = nullSafeCompare( o1Module, o2Module );
    if ( result != null ) {
      return result;
    }
    Integer o1OrderNum = orderedModules.get( o1Module );
    Integer o2OrderNum = orderedModules.get( o2Module );
    result = nullSafeCompare( o1OrderNum, o2OrderNum );
    if ( result != null ) {
      return result;
    }
    return o1Module.compareTo( o2Module );
  }

  @Override public int compare( RuntimeTest o1, RuntimeTest o2 ) {
    Integer result = compareModuleNames( o1.getModule(), o2.getModule() );
    if ( result != 0 ) {
      return result;
    }
    String o1Id = o1.getId();
    String o2Id = o2.getId();
    result = nullSafeCompare( o1Id, o2Id );
    if ( result == null ) {
      result = o1Id.compareTo( o2Id );
    }
    return result;
  }
}
