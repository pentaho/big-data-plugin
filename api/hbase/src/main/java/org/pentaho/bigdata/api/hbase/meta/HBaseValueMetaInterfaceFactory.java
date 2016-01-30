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
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Created by bryan on 1/21/16.
 */
public interface HBaseValueMetaInterfaceFactory {
  HBaseValueMetaInterface createHBaseValueMetaInterface( String family, String column, String alias, int type,
                                                         int length, int precision )
    throws IllegalArgumentException;

  HBaseValueMetaInterface createHBaseValueMetaInterface( String name, int type, int length, int precision )
    throws IllegalArgumentException;

  List<HBaseValueMetaInterface> createListFromRepository( Repository rep, ObjectId id_step ) throws KettleException;

  HBaseValueMetaInterface createFromRepository( Repository rep, ObjectId id_step, int count ) throws KettleException;

  List<HBaseValueMetaInterface> createListFromNode( Node stepNode ) throws KettleXMLException;

  HBaseValueMetaInterface createFromNode( Node metaNode ) throws KettleXMLException;

  HBaseValueMetaInterface copy( HBaseValueMetaInterface hBaseValueMetaInterface );
}
