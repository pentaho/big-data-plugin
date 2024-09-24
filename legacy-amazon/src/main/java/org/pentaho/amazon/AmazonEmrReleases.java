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


/**
 * Created by Aliaksandr_Zhuk on 1/20/2018.
 */
public enum AmazonEmrReleases {
  EMR_700( "emr-7.0.0" ),
  EMR_5360( "emr-5.36.0" ),
  EMR_5110( "emr-5.11.0" ),
  EMR_5100( "emr-5.10.0" ),
  EMR_590( "emr-5.9.0" ),
  EMR_580( "emr-5.8.0" ),
  EMR_570( "emr-5.7.0" ),
  EMR_560( "emr-5.6.0" ),
  EMR_550( "emr-5.5.0" ),
  EMR_540( "emr-5.4.0" ),
  EMR_531( "emr-5.3.1" ),
  EMR_530( "emr-5.3.0" ),
  EMR_522( "emr-5.2.2" ),
  EMR_521( "emr-5.2.1" ),
  EMR_520( "emr-5.2.0" ),
  EMR_510( "emr-5.1.0" ),
  EMR_503( "emr-5.0.3" ),
  EMR_500( "emr-5.0.0" ),
  EMR_492( "emr-4.9.2" ),
  EMR_491( "emr-4.9.1" ),
  EMR_484( "emr-4.8.4" ),
  EMR_483( "emr-4.8.3" ),
  EMR_482( "emr-4.8.2" ),
  EMR_480( "emr-4.8.0" ),
  EMR_472( "emr-4.7.2" ),
  EMR_471( "emr-4.7.1" ),
  EMR_470( "emr-4.7.0" ),
  EMR_460( "emr-4.6.0" ),
  EMR_450( "emr-4.5.0" ),
  EMR_440( "emr-4.4.0" ),
  EMR_430( "emr-4.3.0" ),
  EMR_420( "emr-4.2.0" ),
  EMR_410( "emr-4.1.0" ),
  EMR_400( "emr-4.0.0" );

  private final String emrRelease;

  AmazonEmrReleases( String emrRelease ) {
    this.emrRelease = emrRelease;
  }

  public String getEmrRelease() {
    return emrRelease;
  }
}
