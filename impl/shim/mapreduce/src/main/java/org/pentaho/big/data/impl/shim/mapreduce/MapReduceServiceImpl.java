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

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.mapreduce.MapReduceExecutionException;
import org.pentaho.bigdata.api.mapreduce.MapReduceJarInfo;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobBuilder;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobSimple;
import org.pentaho.bigdata.api.mapreduce.MapReduceService;
import org.pentaho.bigdata.api.mapreduce.PentahoMapReduceJobBuilder;
import org.pentaho.bigdata.api.mapreduce.TransformationVisitorService;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entries.hadoopjobexecutor.JarUtility;
import org.pentaho.hadoop.PluginPropertiesUtil;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * Created by bryan on 12/1/15.
 */
public class MapReduceServiceImpl implements MapReduceService {
  public static final Class<?> PKG = MapReduceServiceImpl.class;
  private final NamedCluster namedCluster;
  private final HadoopShim hadoopShim;
  private final HadoopConfiguration hadoopConfiguration;
  private final ExecutorService executorService;
  private final List<TransformationVisitorService> visitorServices;
  private final JarUtility jarUtility;
  private final PluginPropertiesUtil pluginPropertiesUtil;
  private final PluginRegistry pluginRegistry;

  public MapReduceServiceImpl( NamedCluster namedCluster, HadoopConfiguration hadoopConfiguration,
                               ExecutorService executorService, List<TransformationVisitorService> visitorServices ) {
    this( namedCluster, hadoopConfiguration, executorService, new JarUtility(), new PluginPropertiesUtil(),
      PluginRegistry.getInstance(), visitorServices );
  }

  public MapReduceServiceImpl( NamedCluster namedCluster, HadoopConfiguration hadoopConfiguration,
                               ExecutorService executorService, JarUtility jarUtility,
                               PluginPropertiesUtil pluginPropertiesUtil, PluginRegistry pluginRegistry,
                               List<TransformationVisitorService> visitorServices ) {
    this.namedCluster = namedCluster;
    this.hadoopConfiguration = hadoopConfiguration;
    this.hadoopShim = hadoopConfiguration.getHadoopShim();
    this.executorService = executorService;
    this.jarUtility = jarUtility;
    this.pluginPropertiesUtil = pluginPropertiesUtil;
    this.pluginRegistry = pluginRegistry;
    this.visitorServices = visitorServices;
  }

  @Override
  public MapReduceJobSimple executeSimple( URL resolvedJarUrl, String driverClass, final String commandLineArgs )
    throws MapReduceExecutionException {
    final Class<?> mainClass = locateDriverClass( driverClass, resolvedJarUrl, hadoopShim );
    return new FutureMapReduceJobSimpleImpl( executorService, mainClass, commandLineArgs );
  }

  @Override
  public MapReduceJobBuilder createJobBuilder( final LogChannelInterface log, VariableSpace variableSpace ) {
    return new MapReduceJobBuilderImpl( namedCluster, hadoopShim, log, variableSpace );
  }

  @Override
  public PentahoMapReduceJobBuilder createPentahoMapReduceJobBuilder( LogChannelInterface log,
                                                                      VariableSpace variableSpace )
    throws IOException {
    PluginInterface pluginInterface =
      pluginRegistry.findPluginWithId( LifecyclePluginType.class, HadoopSpoonPlugin.PLUGIN_ID );
    Properties pmrProperties;
    try {
      pmrProperties = pluginPropertiesUtil.loadPluginProperties( pluginInterface );
      return new PentahoMapReduceJobBuilderImpl( namedCluster, hadoopConfiguration, log, variableSpace, pluginInterface,
        pmrProperties, visitorServices );
    } catch ( KettleFileException e ) {
      throw new IOException( e );
    }
  }

  @Override
  public MapReduceJarInfo getJarInfo( URL resolvedJarUrl ) throws IOException, ClassNotFoundException {
    ClassLoader classLoader = getClass().getClassLoader();
    List<Class<?>> classesInJarWithMain = jarUtility.getClassesInJarWithMain( resolvedJarUrl.toExternalForm(), classLoader );
    List<String> classNamesInJarWithMain = new ArrayList<>( classesInJarWithMain.size() );
    for ( Class<?> aClass : classesInJarWithMain ) {
      classNamesInJarWithMain.add( aClass.getCanonicalName() );
    }

    final List<String> finalClassNamesInJarWithMain = Collections.unmodifiableList( classNamesInJarWithMain );

    Class<?> mainClassFromManifest = null;
    try {
      mainClassFromManifest = jarUtility.getMainClassFromManifest( resolvedJarUrl, classLoader );
    } catch ( Exception e ) {
      // Ignore
    }

    final String mainClassName = mainClassFromManifest != null ? mainClassFromManifest.getCanonicalName() : null;

    return new MapReduceJarInfo() {
      @Override
      public List<String> getClassesWithMain() {
        return finalClassNamesInJarWithMain;
      }

      @Override
      public String getMainClass() {
        return mainClassName;
      }
    };
  }

  @VisibleForTesting
  Class<?> locateDriverClass( String driverClass, final URL resolvedJarUrl, final HadoopShim shim )
    throws MapReduceExecutionException {
    try {
      if ( Utils.isEmpty( driverClass ) ) {
        List<Class<?>> mainClasses =
            jarUtility.getClassesInJarWithMain( resolvedJarUrl.toExternalForm(), shim.getClass().getClassLoader() );   
        Class<?> mainClass = jarUtility.getMainClassFromManifest( resolvedJarUrl, shim.getClass().getClassLoader() );
        if ( mainClass == null ) {
          if ( mainClasses.size() == 1 ) {
            return mainClasses.get( 0 );
          } else if ( mainClasses.isEmpty() ) {
            throw new MapReduceExecutionException( BaseMessages.getString( PKG, "MapReduceServiceImpl.DriverClassNotSpecified" ) );
          } else {
            throw new MapReduceExecutionException( BaseMessages.getString( PKG, "MapReduceServiceImpl.MultipleDriverClasses" ) );
          }
        }
        return mainClass;
      } else {
        return jarUtility.getClassByName( driverClass, resolvedJarUrl, shim.getClass().getClassLoader() );
      }
    } catch ( Exception e ) {
      if ( e instanceof MapReduceExecutionException ) {
        throw (MapReduceExecutionException) e;
      } else {
        throw new MapReduceExecutionException( e );
      }
    }
  }
}
