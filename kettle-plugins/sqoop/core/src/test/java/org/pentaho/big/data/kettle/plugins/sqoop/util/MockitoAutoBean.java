/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
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
