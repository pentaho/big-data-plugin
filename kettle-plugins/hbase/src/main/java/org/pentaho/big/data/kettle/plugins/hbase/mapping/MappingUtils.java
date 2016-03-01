/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import java.io.IOException;

import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.hbase.HBaseConnectionException;
import org.pentaho.big.data.kettle.plugins.hbase.input.Messages;
import org.pentaho.bigdata.api.hbase.HBaseConnection;

public class MappingUtils {

  public static MappingAdmin getMappingAdmin( ConfigurationProducer cProducer ) throws HBaseConnectionException {
    HBaseConnection hbConnection = null;
    try {
      hbConnection = cProducer.getHBaseConnection();
      hbConnection.checkHBaseAvailable();
      return new MappingAdmin( hbConnection );
    } catch ( ClusterInitializationException | IOException e ) {
      throw new HBaseConnectionException( Messages.getString( "MappingDialog.Error.Message.UnableToConnect" ), e );
    }
  }

}
