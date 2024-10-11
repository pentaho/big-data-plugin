/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Aliaksandr_Zhuk on 1/26/2018.
 */
public class InstanceType implements Comparable<InstanceType> {

  private String type;
  private String instanceFamily;

  public InstanceType( String type, String instanceFamily ) {
    this.type = type;
    this.instanceFamily = instanceFamily;
  }

  public String getType() {
    return type;
  }

  public void setType( String type ) {
    this.type = type;
  }

  public String getInstanceFamily() {
    return instanceFamily;
  }

  public void setInstanceFamily( String instanceFamily ) {
    this.instanceFamily = instanceFamily;
  }

  public static String createDescription( InstanceType type ) {
    return type.getType() + " (" + type.getInstanceFamily() + ")";
  }

  public static String getTypeFromDescription( String description ) {
    return description.trim().split( "\\s+" )[ 0 ];
  }

  @Override
  public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }

    InstanceType instanceType = (InstanceType) o;

    return type != null ? type.equals( instanceType.type ) : false;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + ( ( type == null ) ? 0 : type.hashCode() );
    return result;
  }

  @Override
  public int compareTo( InstanceType instanceType ) {
    return extractInt( type ) - extractInt( instanceType.getType() );
  }

  private int extractInt( String type ) {
    String number = type.split( "\\." )[ 1 ].replaceAll( "\\D", "" );
    return number.isEmpty() ? 0 : Integer.parseInt( number );
  }

  public static List<String> sortInstanceTypes( List<InstanceType> instances ) {

    List<InstanceType> instanceGroup;
    List<String> instanceGroupTypes;
    List<String> sortedInstances = new ArrayList<>();
    Collections.sort( instances, ( o1, o2 ) -> o1.getType().compareTo( o2.getType() ) );

    while ( instances.size() > 0 ) {
      instanceGroup = instances.stream()
        .filter( e -> instances.get( 0 ).getType().split( "\\." )[ 0 ].equals( e.getType().split( "\\." )[ 0 ] ) )
        .collect(
          Collectors.toList() );

      instances.removeAll( instanceGroup );
      Collections.sort( instanceGroup );

      instanceGroupTypes = instanceGroup.stream().map( e -> e.getType() ).collect( Collectors.toList() );

      sortedInstances.addAll( instanceGroupTypes );
    }
    return sortedInstances;
  }
}
