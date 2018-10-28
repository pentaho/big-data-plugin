/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.mapreduce;

import org.pentaho.bigdata.api.mapreduce.MapReduceExecutionException;
import org.pentaho.di.job.entries.hadoopjobexecutor.NoExitSecurityManager;
import org.pentaho.di.job.entries.hadoopjobexecutor.SecurityManagerStack;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by bryan on 12/7/15.
 */
public class FutureMapReduceJobSimpleRunnable implements Runnable {
  private static final SecurityManagerStack securityManagerStack = SecurityManagerStack.getInstance();
  private final Class<?> mainClass;
  private final String commandLineArgs;
  private final AtomicBoolean complete;
  private final AtomicInteger status;
  private final AtomicReference<MapReduceExecutionException> exceptionAtomicReference;

  public FutureMapReduceJobSimpleRunnable( Class<?> mainClass, String commandLineArgs, AtomicBoolean complete,
                                           AtomicInteger status,
                                           AtomicReference<MapReduceExecutionException> exceptionAtomicReference ) {
    this.mainClass = mainClass;
    this.commandLineArgs = commandLineArgs;
    this.complete = complete;
    this.status = status;
    this.exceptionAtomicReference = exceptionAtomicReference;
  }

  @Override public void run() {
    final NoExitSecurityManager nesm = new NoExitSecurityManager( System.getSecurityManager() );
    securityManagerStack.setSecurityManager( nesm );
    try {
      nesm.addBlockedThread( Thread.currentThread() );
      try {
        executeMainMethod( mainClass, commandLineArgs );
        updateWithStatus( 0, null );
      } finally {
        nesm.removeBlockedThread( Thread.currentThread() );
        securityManagerStack.removeSecurityManager( nesm );
      }
    } catch ( Throwable ex ) {
      if ( ex instanceof InvocationTargetException ) {
        ex = ( (InvocationTargetException) ex ).getTargetException();
      }
      if ( ex instanceof NoExitSecurityManager.NoExitSecurityException ) {
        updateWithStatus( ( (NoExitSecurityManager.NoExitSecurityException) ex ).getStatus(), null );
      } else {
        updateWithStatus( -1, new MapReduceExecutionException( ex ) );
      }
    }
  }

  /**
   * Execute the main method of the provided class with the current command line arguments.
   *
   * @param clazz Class with main method to execute
   * @throws NoSuchMethodException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  protected void executeMainMethod( Class<?> clazz, String commandLineArgs )
    throws NoSuchMethodException, IllegalAccessException,
    InvocationTargetException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader( clazz.getClassLoader() );
      Method mainMethod = clazz.getMethod( "main", new Class[] { String[].class } );
      Object[] args =
        ( commandLineArgs != null ) ? new Object[] { commandLineArgs.split( " " ) } : new Object[] { new String[ 0 ] };
      mainMethod.invoke( clazz.newInstance(), args );
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  private void updateWithStatus( int status, MapReduceExecutionException exception ) {
    complete.set( true );
    this.status.set( status );
    exceptionAtomicReference.set( exception );
  }

  public AtomicBoolean getComplete() {
    return complete;
  }

  public AtomicInteger getStatus() {
    return status;
  }

  public AtomicReference<MapReduceExecutionException> getExceptionAtomicReference() {
    return exceptionAtomicReference;
  }
}
