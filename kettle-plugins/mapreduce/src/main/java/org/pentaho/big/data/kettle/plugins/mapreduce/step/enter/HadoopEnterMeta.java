/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.mapreduce.step.enter;

import org.pentaho.big.data.kettle.plugins.mapreduce.DialogClassUtil;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.trans.steps.injector.InjectorMeta;

@Step( id = "HadoopEnterPlugin", image = "MRI.svg", name = "HadoopEnterPlugin.Name",
    description = "HadoopEnterPlugin.Description",
    documentationUrl = "Products/MapReduce_Input",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    i18nPackageName = "org.pentaho.di.trans.steps.hadoopenter" )
@InjectionSupported( localizationPrefix = "HadoopEnterPlugin.Injection." )
public class HadoopEnterMeta extends InjectorMeta {
  @SuppressWarnings( "unused" )
  private static Class<?> PKG = HadoopEnterMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$
  public static final String DIALOG_NAME = DialogClassUtil.getDialogClassName( PKG );

  public static final String KEY_FIELDNAME = "key";
  public static final String VALUE_FIELDNAME = "value";

  public HadoopEnterMeta() throws Throwable {
    setDefault();
  }

  @Override public void setDefault() {
    allocate( 2 );

    getFieldname()[ 0 ] = HadoopEnterMeta.KEY_FIELDNAME;
    getFieldname()[ 1 ] = HadoopEnterMeta.VALUE_FIELDNAME;
  }

  @Override public String getDialogClassName() {
    return DIALOG_NAME;
  }

  @Injection( name = "KEY_TYPE" )
  public void setKeyType( int type ) {
    getType()[0] = type;
  }

  @Injection( name = "KEY_LENGTH" )
  public void setKeyLength( int length ) {
    getLength()[0] = length;
  }

  @Injection( name = "KEY_PRECISION" )
  public void setKeyPrecision( int precision ) {
    getPrecision()[0] = precision;
  }

  @Injection( name = "VALUE_TYPE" )
  public void setValueType( int type ) {
    getType()[1] = type;
  }

  @Injection( name = "VALUE_LENGTH" )
  public void setValueLength( int length ) {
    getLength()[1] = length;
  }

  @Injection( name = "VALUE_PRECISION" )
  public void setValuePrecision( int precision ) {
    getPrecision()[1] = precision;
  }
}
