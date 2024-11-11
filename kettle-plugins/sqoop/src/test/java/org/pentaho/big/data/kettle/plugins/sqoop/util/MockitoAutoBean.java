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


package org.pentaho.big.data.kettle.plugins.sqoop.util;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Method;

/**
 * @author nhudak
 */
public class MockitoAutoBean<T> implements Answer<T> {
  private T value;

  public MockitoAutoBean() {
  }

  public MockitoAutoBean( T value ) {
    this();
    this.value = value;
  }

  @SuppressWarnings( "unchecked" )
  @Override public T answer( InvocationOnMock invocation ) throws Throwable {
    Method method = invocation.getMethod();
    if ( method.getParameterTypes().length == 1 ) {
      value = (T) invocation.getArguments()[0];
    }
    return value;
  }

  public T getValue() {
    return value;
  }

  public void setValue( T value ) {
    this.value = value;
  }
}
