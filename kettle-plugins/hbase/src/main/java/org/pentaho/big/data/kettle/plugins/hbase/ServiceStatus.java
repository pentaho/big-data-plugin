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

package org.pentaho.big.data.kettle.plugins.hbase;


/**
 * Helper class that shows HBaseService status of a Step.
 */
public class ServiceStatus {

  public static ServiceStatus OK = new ServiceStatus();

  private boolean ok = true;
  private Exception exception;

  private ServiceStatus() {
  }

  private ServiceStatus( Exception exception ) {
    this.ok = false;
    this.exception = exception;
  }

  public boolean isOk() {
    return ok;
  }

  public Exception getException() {
    return exception;
  }

  public static ServiceStatus notOk( Exception e ) {
    return new ServiceStatus( e );
  }
}
