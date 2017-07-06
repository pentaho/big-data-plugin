/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.parquet.input;

public class PentahoParquetReadSupport  {
  /*extends ReadSupport<Group>
  public static MessageType schema; //TODO

  public PentahoParquetReadSupport() {
  }

  @Override
  public ReadContext init( Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema ) {
    this.schema = fileSchema;
    return new ReadContext( fileSchema, new HashMap<String, String>() );
  }

  @Override
  public RecordMaterializer<Group> prepareForRead( Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema, org.apache.parquet.hadoop.api.ReadSupport.ReadContext readContext ) {
    return new GroupRecordConverter( schema );
  }*/
}
