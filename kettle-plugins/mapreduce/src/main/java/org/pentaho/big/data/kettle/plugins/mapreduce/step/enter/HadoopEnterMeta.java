/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.mapreduce.step.enter;

import org.pentaho.big.data.kettle.plugins.mapreduce.DialogClassUtil;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.trans.steps.injector.InjectorMeta;

@Step( id = "HadoopEnterPlugin", image = "MRI.svg", name = "HadoopEnterPlugin.Name",
    description = "HadoopEnterPlugin.Description",
    documentationUrl = "mk-95pdia003/pdi-transformation-steps/mapreduce-input",
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
