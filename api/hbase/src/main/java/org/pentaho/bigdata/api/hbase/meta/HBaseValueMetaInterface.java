/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.bigdata.api.hbase.meta;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;

/**
 * Created by bryan on 1/19/16.
 */
public interface HBaseValueMetaInterface extends ValueMetaInterface {
  boolean isKey();

  void setKey( boolean key );

  String getAlias();

  void setAlias( String alias );

  String getColumnName();

  void setColumnName( String columnName );

  String getColumnFamily();

  void setColumnFamily( String family );

  void setHBaseTypeFromString( String hbaseType ) throws IllegalArgumentException;

  String getHBaseTypeDesc();

  Object decodeColumnValue( byte[] rawColValue ) throws KettleException;

  String getTableName();

  void setTableName( String tableName );

  String getMappingName();

  void setMappingName( String mappingName );

  boolean getIsLongOrDouble();

  void setIsLongOrDouble( boolean ld );

  byte[] encodeColumnValue( Object o, ValueMetaInterface valueMetaInterface ) throws KettleException;

  void getXml( StringBuilder stringBuilder );

  void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int count ) throws KettleException;
}
