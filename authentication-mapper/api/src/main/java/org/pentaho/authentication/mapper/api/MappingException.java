/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.authentication.mapper.api;

/**
 * Created by bryan on 3/18/16.
 */
public class MappingException extends Exception {
  public MappingException() {
  }

  public MappingException( String message ) {
    super( message );
  }

  public MappingException( String message, Throwable cause ) {
    super( message, cause );
  }

  public MappingException( Throwable cause ) {
    super( cause );
  }

  @FunctionalInterface
  public interface Function<T, R> {
    R apply( T t ) throws MappingException;
  }

  @FunctionalInterface
  public interface Supplier<R> {
    R get() throws MappingException;
  }
}
