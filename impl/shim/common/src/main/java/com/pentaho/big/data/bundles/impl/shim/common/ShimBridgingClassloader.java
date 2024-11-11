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


package com.pentaho.big.data.bundles.impl.shim.common;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleReference;
import org.osgi.framework.wiring.BundleWiring;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginRegistry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.List;

/**
 * Created by bryan on 6/4/15.
 *
 * @deprecated
 */
@Deprecated
public class ShimBridgingClassloader extends ClassLoader implements BundleReference {
  public static final String HADOOP_SPOON_PLUGIN = "HadoopSpoonPlugin";
  private static PluginClassloaderGetter pluginClassloaderGetter = new PluginClassloaderGetter();
  private final BundleWiring bundleWiring;
  private final PublicLoadResolveClassLoader bundleWiringClassloader;

  public ShimBridgingClassloader( ClassLoader parentClassLoader, BundleContext bundleContext ) {
    super( parentClassLoader );
    this.bundleWiring = (BundleWiring) bundleContext.getBundle().adapt( BundleWiring.class );
    this.bundleWiringClassloader = new PublicLoadResolveClassLoader( bundleWiring.getClassLoader() );
  }

  public static Object create( BundleContext bundleContext, String className, List<Object> arguments )
    throws KettlePluginException, ClassNotFoundException, IllegalAccessException, InstantiationException,
    InvocationTargetException {
    ShimBridgingClassloader shimBridgingClassloader =
      new ShimBridgingClassloader( pluginClassloaderGetter.getPluginClassloader(
        LifecyclePluginType.class.getCanonicalName(), HADOOP_SPOON_PLUGIN ), bundleContext );
    Class<?> clazz = Class.forName( className, true, shimBridgingClassloader );
    if ( arguments == null || arguments.size() == 0 ) {
      return clazz.newInstance();
    }
    for ( Constructor<?> constructor : clazz.getConstructors() ) {
      Class<?>[] parameterTypes = constructor.getParameterTypes();
      if ( parameterTypes.length == arguments.size() ) {
        boolean match = true;
        for ( int i = 0; i < parameterTypes.length; i++ ) {
          Object o = arguments.get( i );
          if ( o != null && !parameterTypes[ i ].isInstance( o ) ) {
            match = false;
            break;
          }
        }
        if ( match ) {
          return constructor.newInstance( arguments.toArray() );
        }
      }
    }
    throw new InstantiationException(
      "Unable to find constructor for class " + className + " with arguments " + arguments );
  }

  @VisibleForTesting
  static PluginClassloaderGetter getPluginClassloaderGetter() {
    return pluginClassloaderGetter;
  }

  @VisibleForTesting
  static void setPluginClassloaderGetter( PluginClassloaderGetter pluginClassloaderGetter ) {
    ShimBridgingClassloader.pluginClassloaderGetter = pluginClassloaderGetter;
  }

  @Override
  protected Class<?> findClass( String name ) throws ClassNotFoundException {
    int lastIndexOfDot = name.lastIndexOf( '.' );
    final String packageName;
    final String translatedPath;
    final String translatedName;
    if ( lastIndexOfDot >= 0 ) {
      packageName = name.substring( 0, lastIndexOfDot );
      if ( getPackage( packageName ) == null ) {
        definePackage( packageName, null, null, null, null, null, null, null );
      }
      translatedPath = "/" + packageName.replace( '.', '/' );
      translatedName = name.substring( lastIndexOfDot + 1 ) + ".class";
    } else {
      packageName = "";
      translatedPath = "/";
      translatedName = name;
    }
    if ( getPackage( packageName ) == null ) {
      definePackage( packageName, null, null, null, null, null, null, null );
    }
    List<URL> entries = bundleWiring.findEntries( translatedPath, translatedName, 0 );
    if ( entries.size() == 1 ) {
      byte[] bytes;
      try ( ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream() ) {
        IOUtils.copy( entries.get( 0 ).openStream(), byteArrayOutputStream );
        bytes = byteArrayOutputStream.toByteArray();
      } catch ( IOException e ) {
        throw new ClassNotFoundException( "Unable to define class", e );
      }
      return defineClass( name, bytes, 0, bytes.length );
    }
    throw new ClassNotFoundException();
  }

  @Override public URL getResource( String name ) {
    int lastIndexOf = name.lastIndexOf( '/' );

    List<URL> entries;
    if ( lastIndexOf > 0 ) {
      entries = bundleWiring.findEntries( name.substring( 0, lastIndexOf ), name.substring( lastIndexOf + 1 ), 0 );
    } else {
      entries = bundleWiring.findEntries( "/", name, 0 );
    }
    if ( entries.size() == 1 ) {
      return entries.get( 0 );
    }
    URL resource = bundleWiringClassloader.getResource( name );
    if ( resource == null ) {
      resource = super.getResource( name );
    }
    return resource;
  }

  @Override
  public Class<?> loadClass( String name, boolean resolve ) throws ClassNotFoundException {
    Class<?> result = null;
    synchronized ( this ) {
      result = findLoadedClass( name );
    }
    if ( result == null ) {
      try {
        result = findClass( name );
      } catch ( Exception e ) {
        // Ignore
      }
    }
    if ( result == null ) {
      try {
        Class<?> osgiProvidedClass = bundleWiringClassloader.loadClass( name, resolve );
        if ( osgiProvidedClass.getClassLoader() == PluginRegistry.class.getClassLoader() ) {
          // Give parent a chance to supercede the system classloader (workaround for boot delegation of packages we
          // should have loaded from the parent)
          try {
            return super.loadClass( name, resolve );
          } catch ( Exception e ) {
            // Ignore
          }
        }
        return osgiProvidedClass;
      } catch ( Exception e ) {
        // Ignore
      }
    }
    if ( result == null ) {
      return super.loadClass( name, resolve );
    }
    if ( resolve ) {
      resolveClass( result );
    }
    return result;
  }

  @Override
  public Bundle getBundle() {
    return this.bundleWiring.getBundle();
  }

  /**
   * Trivial classloader subclass that lets us call loadClass with a resolve parameter
   */
  @VisibleForTesting
  static class PublicLoadResolveClassLoader extends ClassLoader {
    public PublicLoadResolveClassLoader( ClassLoader parent ) {
      super( parent );
    }

    @Override
    public Class<?> loadClass( String name, boolean resolve ) throws ClassNotFoundException {
      return super.loadClass( name, resolve );
    }
  }
}

