package com.pentaho.big.data.bundles.impl.shim.common;

import org.apache.commons.io.IOUtils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.wiring.BundleWiring;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeInterface;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.List;

/**
 * Created by bryan on 6/4/15.
 */
public class ShimBridgingClassloader extends ClassLoader {
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
    ShimBridgingClassloader shimBridgingClassloader = new ShimBridgingClassloader( getPluginClassloader(
      LifecyclePluginType.class.getCanonicalName(), "HadoopSpoonPlugin" ), bundleContext );
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

  /**
   * Gets the classloader for the specified plugin, blocking until the plugin becomes available the feature watcher will
   * kill us after a while anyway
   *
   * @param pluginType the plugin type (Specified as a string so that we can get the classloader for plugin types OSGi
   *                   doesn't know about)
   * @param pluginId   the plugin id
   * @return
   * @throws KettlePluginException
   * @throws InterruptedException
   */
  private static ClassLoader getPluginClassloader( String pluginType, String pluginId )
    throws KettlePluginException {
    Class<? extends PluginTypeInterface> pluginTypeInterface = null;
    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    while ( true ) {
      synchronized ( pluginRegistry ) {
        if ( pluginTypeInterface == null ) {
          for ( Class<? extends PluginTypeInterface> potentialPluginTypeInterface : pluginRegistry.getPluginTypes() ) {
            if ( pluginType.equals( potentialPluginTypeInterface.getCanonicalName() ) ) {
              pluginTypeInterface = potentialPluginTypeInterface;
            }
          }
        }
        PluginInterface plugin = pluginRegistry.getPlugin( pluginTypeInterface, pluginId );
        if ( plugin != null ) {
          return pluginRegistry.getClassLoader( plugin );
        }
        try {
          pluginRegistry.wait();
        } catch ( InterruptedException e ) {
          throw new KettlePluginException( e );
        }
      }
    }
  }

  @Override
  protected Class<?> findClass( String name ) throws ClassNotFoundException {
    int lastIndexOfDot = name.lastIndexOf( '.' );
    String translatedPath = "/" + name.substring( 0, lastIndexOfDot ).replace( '.', '/' );
    String translatedName = name.substring( lastIndexOfDot + 1 ) + ".class";
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

      }
    }
    if ( result == null ) {
      try {
        return bundleWiringClassloader.loadClass( name, resolve );
      } catch ( Exception e ) {

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

  /**
   * Trivial classloader subclass that lets us call loadClass with a resolve parameter
   */
  private static class PublicLoadResolveClassLoader extends ClassLoader {
    public PublicLoadResolveClassLoader( ClassLoader parent ) {
      super( parent );
    }

    @Override
    public Class<?> loadClass( String name, boolean resolve ) throws ClassNotFoundException {
      return super.loadClass( name, resolve );
    }
  }
}

