/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by Aliaksandr_Zhuk on 2/14/2018.
 */
@RunWith( MockitoJUnitRunner.class )
public class MappingAdminTest {

  private TransMeta transMeta;
  private BaseStepMeta stepMeta;
  private StepMeta parentStepMeta;

  @Before
  public void setUp() throws KettleException {
    KettleEnvironment.init();
    transMeta = Mockito.spy( new TransMeta() );
    stepMeta = Mockito.spy( new BaseStepMeta() );
    parentStepMeta = Mockito.spy( new StepMeta() );
    parentStepMeta.setParentTransMeta( transMeta );
    stepMeta.setParentStepMeta( parentStepMeta );
  }

  @Test
  public void testGetTableNameFromVariable_whenVariableValueExists() {

    String expectedTableName = "hbweblogs";

    transMeta.setVariable( "hb_weblogs", "hbweblogs" );

    String tableName = MappingAdmin.getTableNameFromVariable( stepMeta, "${hb_weblogs}" );

    assertEquals( expectedTableName, tableName );
  }

  @Test
  public void testGetTableNameFromVariable_whenNoVariable() {

    String expectedTableName = "hbweblogs";
    String expectedResult = "${hb_weblogs}";

    String tableName = MappingAdmin.getTableNameFromVariable( stepMeta, "${hb_weblogs}" );

    assertNotEquals( expectedTableName, tableName );
    assertEquals( expectedResult, tableName );
  }
}
