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

package com.pentaho.big.data.bundles.impl.shim.common;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeInterface;
import org.pentaho.di.core.plugins.StepPluginType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/5/15.
 *
 * @deprecated
 */
@Deprecated
public class PluginClassloaderGetterTest {
  private PluginRegistry pluginRegistry;
  private PluginClassloaderGetter pluginClassloaderGetter;

  @Before
  public void setup() {
    pluginRegistry = mock( PluginRegistry.class );
    pluginClassloaderGetter = new PluginClassloaderGetter( pluginRegistry );
  }

  @Test
  public void testGetPluginClassloader() throws InterruptedException {
    final AtomicReference<ClassLoader> classLoaderAtomicReference = new AtomicReference<>( null );
    final AtomicReference<KettlePluginException> exceptionAtomicReference = new AtomicReference<>( null );
    final ClassLoader classLoader = mock( ClassLoader.class );
    when( pluginRegistry.getPluginTypes() ).thenAnswer( new Answer<List<Class<? extends PluginTypeInterface>>>() {
      private int invocationNum = 0;

      @Override public List<Class<? extends PluginTypeInterface>> answer( InvocationOnMock invocation )
        throws Throwable {
        if ( invocationNum == 0 ) {
          new Thread( new Runnable() {
            @Override public void run() {
              synchronized ( pluginRegistry ) {
                pluginRegistry.notifyAll();
              }
            }
          } ).start();
          invocationNum++;
          return new ArrayList<>();
        } else if ( invocationNum == 1 ) {
          invocationNum++;
          return new ArrayList<Class<? extends PluginTypeInterface>>(
            Arrays.asList( StepPluginType.class, LifecyclePluginType.class ) );
        } else {
          throw new Exception( "Only expected to be called twice" );
        }
      }
    } );

    when( pluginRegistry.getPlugin( LifecyclePluginType.class, ShimBridgingClassloader.HADOOP_SPOON_PLUGIN ) )
      .thenAnswer(
        new Answer<PluginInterface>() {
          private int invocationNum = 0;

          @Override public PluginInterface answer( InvocationOnMock invocation ) throws Throwable {
            if ( invocationNum == 0 ) {
              new Thread( new Runnable() {
                @Override public void run() {
                  synchronized ( pluginRegistry ) {
                    pluginRegistry.notifyAll();
                  }
                }
              } ).start();
              invocationNum++;
              return null;
            } else if ( invocationNum == 1 ) {
              invocationNum++;
              PluginInterface pluginInterface = mock( PluginInterface.class );
              when( pluginRegistry.getClassLoader( pluginInterface ) ).thenReturn( classLoader );
              return pluginInterface;
            } else {
              throw new Exception( "Only expected to be called twice" );
            }
          }
        }
      );

    Thread thread = new Thread( new Runnable() {
      @Override public void run() {
        try {
          classLoaderAtomicReference.set( pluginClassloaderGetter
            .getPluginClassloader( LifecyclePluginType.class.getCanonicalName(),
              ShimBridgingClassloader.HADOOP_SPOON_PLUGIN ) );
        } catch ( KettlePluginException e ) {
          exceptionAtomicReference.set( e );
        }
      }
    } );
    thread.start();
    thread.join();
    assertNull( exceptionAtomicReference.get() );
    assertEquals( classLoader, classLoaderAtomicReference.get() );
  }

  @Test
  public void testGetPluginClassloaderInterrupted() throws InterruptedException {
    final AtomicReference<ClassLoader> classLoaderAtomicReference = new AtomicReference<>( null );
    final AtomicReference<KettlePluginException> exceptionAtomicReference = new AtomicReference<>( null );

    final Thread thread = new Thread( new Runnable() {
      @Override public void run() {
        try {
          classLoaderAtomicReference.set( pluginClassloaderGetter
            .getPluginClassloader( LifecyclePluginType.class.getCanonicalName(),
              ShimBridgingClassloader.HADOOP_SPOON_PLUGIN ) );
        } catch ( KettlePluginException e ) {
          exceptionAtomicReference.set( e );
        }
      }
    } );

    when( pluginRegistry.getPluginTypes() ).thenAnswer( new Answer<List<Class<? extends PluginTypeInterface>>>() {
      @Override public List<Class<? extends PluginTypeInterface>> answer( InvocationOnMock invocation )
        throws Throwable {
        new Thread( new Runnable() {
          @Override public void run() {
            synchronized ( pluginRegistry ) {
              thread.interrupt();
            }
          }
        } ).start();
        return new ArrayList<>();
      }
    } );

    thread.start();
    thread.join();
    assertTrue( exceptionAtomicReference.get() instanceof KettlePluginException );
    assertNull( classLoaderAtomicReference.get() );
  }
}
