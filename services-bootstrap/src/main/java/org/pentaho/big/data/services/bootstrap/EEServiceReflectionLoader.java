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

package org.pentaho.big.data.services.bootstrap;

import org.pentaho.authentication.mapper.api.AuthenticationMappingManager;
import org.pentaho.hadoop.shim.api.HadoopClientServices;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceService;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.big.data.impl.shim.mapreduce.TransformationVisitorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.lang.reflect.Constructor;


/**
 * Utility class for loading EE services via reflection.
 * This allows the CE plugin to dynamically load EE functionality when EE jars are present,
 * while gracefully degrading when they are absent.
 */
public class EEServiceReflectionLoader {

  private static final Logger logger = LoggerFactory.getLogger( EEServiceReflectionLoader.class );

  // EE Class Names - defined as constants to avoid typos and enable easy maintenance
  private static final String AUTH_REQUEST_TO_UGI_MAPPING_SERVICE =
    "com.pentaho.big.data.ee.secure.impersonation.service.AuthRequestToUGIMappingService";
  private static final String AUTHENTICATION_MAPPING_MANAGER_IMPL =
    "org.pentaho.authentication.mapper.impl.AuthenticationMappingManagerImpl";
  private static final String SIMPLE_MAPPING =
    "com.pentaho.big.data.ee.secure.impersonation.service.impersonation.SimpleMapping";

  // EE HDFS Class Names
  private static final String IMPERSONATING_HADOOP_FILE_SYSTEM_FACTORY_IMPL =
    "com.pentaho.hadoop.shim.hdfs.security.impersonation.ImpersonatingHadoopFileSystemFactoryImpl";
  private static final String KNOX_HADOOP_FILE_SYSTEM_FACTORY_IMPL =
    "com.pentaho.hadoop.shim.hdfs.security.knox.KnoxHadoopFileSystemFactoryImpl";

  // EE MapReduce Class Names
  private static final String MAPREDUCE_IMPERSONATION_SERVICE_FACTORY =
    "com.pentaho.hadoop.shim.mapreduce.security.impersonation.MapReduceImpersonationServiceFactory";
  private static final String KNOX_MAPREDUCE_SERVICE_FACTORY =
    "com.pentaho.hadoop.shim.mapreduce.security.impersonation.knox.KnoxMapReduceServiceFactory";

  // EE Sqoop Class Names
  private static final String IMPERSONATING_HADOOP_CLIENT_SERVICES_FACTORY =
    "com.pentaho.hadoop.shim.ImpersonatingHadoopClientServicesFactory";

  /**
   * Check if EE services are available on the classpath.
   * This method attempts to load a core EE class to determine availability.
   *
   * @return true if EE services are available, false otherwise
   */
  public static boolean isEEAvailable() {
    try {
      Class.forName( AUTH_REQUEST_TO_UGI_MAPPING_SERVICE );
      logger.debug( "EE services detected on classpath" );
      return true;
    } catch ( ClassNotFoundException e ) {
      logger.debug( "EE services not available on classpath - running in CE mode" );
      return false;
    }
  }

  /**
   * Load and create an AuthenticationMappingManager using reflection.
   * This replaces the commented authentication manager bootstrap code.
   *
   * @param hadoopShim The hadoop shim instance
   * @return AuthenticationMappingManager instance if EE classes are available, null otherwise
   */
  public static AuthenticationMappingManager loadAuthenticationManager( HadoopShim hadoopShim ) {
    try {
      logger.debug( "Attempting to load EE authentication manager via reflection" );

      // Load EE classes
      Class<?> simpleMappingClass = Class.forName( SIMPLE_MAPPING );
      Class<?> authRequestToUGIClass = Class.forName( AUTH_REQUEST_TO_UGI_MAPPING_SERVICE );
      Class<?> authManagerImplClass = Class.forName( AUTHENTICATION_MAPPING_MANAGER_IMPL );

      // Create SimpleMapping instance
      Constructor<?> simpleMappingConstructor = simpleMappingClass.getDeclaredConstructor();
      Object simpleMapping = simpleMappingConstructor.newInstance();

      // Create AuthRequestToUGIMappingService instance
      Constructor<?> authRequestConstructor = authRequestToUGIClass.getDeclaredConstructor(
        HadoopShim.class,
        Class.forName( "com.pentaho.big.data.ee.secure.impersonation.api.ImpersonationMappingService")
      );
      Object authRequestService = authRequestConstructor.newInstance( hadoopShim, simpleMapping );

      // Create AuthenticationMappingManagerImpl instance
      Constructor<?> authManagerConstructor = authManagerImplClass.getDeclaredConstructor(
        Class.forName( "org.pentaho.authentication.mapper.api.AuthenticationMappingService" )
      );
      Object authManager = authManagerConstructor.newInstance( authRequestService );

      logger.debug( "Successfully loaded EE authentication manager" );
      return (AuthenticationMappingManager) authManager;

    } catch ( ClassNotFoundException e ) {
      logger.debug( "EE authentication classes not found - skipping EE authentication manager" );
      return null;
    } catch ( Exception e ) {
      logger.warn( "Failed to instantiate EE authentication manager via reflection", e );
      return null;
    }
  }

  /**
   * Load EE HDFS filesystem factories using reflection.
   *
   * @param hadoopShim                   The hadoop shim instance
   * @param availableHdfsOptions         List of available HDFS options from shim
   * @param authenticationMappingManager Authentication manager (may be null if not available)
   * @return List of EE HDFS factories if EE classes are available, empty list otherwise
   */
  public static List<HadoopFileSystemFactory> loadEEHDFSFactories( HadoopShim hadoopShim,
                                                                   List<String> availableHdfsOptions,
                                                                   AuthenticationMappingManager authenticationMappingManager ) {
    List<HadoopFileSystemFactory> eeFactories = new ArrayList<>();

    if ( !isEEAvailable() ) {
      logger.debug( "EE services not available - skipping EE HDFS factory loading" );
      return eeFactories;
    }

    try {
      logger.debug( "Attempting to load EE HDFS factories via reflection" );

      // Load EE HDFS factory classes
      Class<?> impersonatingFactoryClass = Class.forName( IMPERSONATING_HADOOP_FILE_SYSTEM_FACTORY_IMPL );
      Class<?> knoxFactoryClass = Class.forName( KNOX_HADOOP_FILE_SYSTEM_FACTORY_IMPL );

      // Create impersonating HDFS factory if available
      if ( availableHdfsOptions.contains( "hdfs_impersonation" ) ) {
        if ( authenticationMappingManager != null ) {
          logger.debug( "Adding 'hdfs_impersonation' factory via reflection" );
          Constructor<?> impersonatingConstructor = impersonatingFactoryClass.getDeclaredConstructor(
            HadoopShim.class,
            Class.forName( "org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface" ), // ShimIdentifier interface
            AuthenticationMappingManager.class
          );

          Object impersonatingFactory = impersonatingConstructor.newInstance(
            hadoopShim,
            hadoopShim.getShimIdentifier(),
            authenticationMappingManager
          );

          eeFactories.add( (HadoopFileSystemFactory) impersonatingFactory );
          logger.debug( "Successfully created hdfs_impersonation factory via reflection" );
        } else {
          logger.debug( "Skipping hdfs_impersonation factory - authentication manager not available" );
        }
      }

      // Create Knox HDFS factory if available  
      if ( availableHdfsOptions.contains( "hdfs_knox" ) ) {
        logger.debug( "Adding 'hdfs_knox' factory via reflection" );
        Constructor<?> knoxConstructor = knoxFactoryClass.getDeclaredConstructor(
          HadoopShim.class,
          Class.forName( "org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface" ) // ShimIdentifier interface
        );

        Object knoxFactory = knoxConstructor.newInstance(
          hadoopShim,
          hadoopShim.getShimIdentifier()
        );

        eeFactories.add( (HadoopFileSystemFactory) knoxFactory );
        logger.debug( "Successfully created hdfs_knox factory via reflection" );
      }

      if ( !eeFactories.isEmpty() ) {
        logger.debug( "Successfully loaded {} EE HDFS factories via reflection", eeFactories.size() );
      }

    } catch ( Exception e ) {
      logger.warn( "Failed to load EE HDFS factories via reflection - continuing without EE HDFS services", e );
    }

    return eeFactories;
  }


  /**
   * Load EE MapReduce service factories using reflection.
   *
   * @param hadoopShim The hadoop shim instance
   * @param availableMapreduceOptions List of available MapReduce options from shim
   * @param authenticationMappingManager Authentication manager (may be null if not available)
   * @param executorService Executor service for MapReduce operations
   * @param visitorServices List of transformation visitor services
   * @return List of EE MapReduce factories if EE classes are available, empty list otherwise
   */
  public static List<NamedClusterServiceFactory<MapReduceService>> loadEEMapReduceFactories(HadoopShim hadoopShim, 
                                                      List<String> availableMapreduceOptions,
                                                      AuthenticationMappingManager authenticationMappingManager,
                                                      ExecutorService executorService,
                                                      List<TransformationVisitorService> visitorServices) {
    List<NamedClusterServiceFactory<MapReduceService>> eeFactories = new ArrayList<>();
    
    if (!isEEAvailable()) {
      logger.debug("EE services not available - skipping EE MapReduce factory loading");
      return eeFactories;
    }
    
    try {
      logger.debug("Attempting to load EE MapReduce factories via reflection");
      
      // Load EE MapReduce factory classes
      Class<?> impersonationFactoryClass = Class.forName(MAPREDUCE_IMPERSONATION_SERVICE_FACTORY);
      Class<?> knoxFactoryClass = Class.forName(KNOX_MAPREDUCE_SERVICE_FACTORY);
      
      // Create impersonating MapReduce factory if available
      if (availableMapreduceOptions.contains("mapreduce_impersonation")) {
        if (authenticationMappingManager != null) {
          logger.debug("Adding 'mapreduce_impersonation' factory via reflection");
          Constructor<?> impersonatingConstructor = impersonationFactoryClass.getDeclaredConstructor(
            HadoopShim.class,
            ExecutorService.class,
            AuthenticationMappingManager.class,
            List.class
          );
          
          Object impersonatingFactory = impersonatingConstructor.newInstance(
            hadoopShim,
            executorService,
            authenticationMappingManager,
            visitorServices
          );
          
          eeFactories.add((NamedClusterServiceFactory<MapReduceService>) impersonatingFactory);
          logger.debug("Successfully created mapreduce_impersonation factory via reflection");
        } else {
          logger.debug("Skipping mapreduce_impersonation factory - authentication manager not available");
        }
      }
      
      // Create Knox MapReduce factory if available  
      if (availableMapreduceOptions.contains("mapreduce_knox")) {
        logger.debug("Adding 'mapreduce_knox' factory via reflection");
        Constructor<?> knoxConstructor = knoxFactoryClass.getDeclaredConstructor(
          HadoopShim.class,
          List.class
        );
        
        Object knoxFactory = knoxConstructor.newInstance(
          hadoopShim,
          visitorServices
        );
        
        eeFactories.add((NamedClusterServiceFactory<MapReduceService>) knoxFactory);
        logger.debug("Successfully created mapreduce_knox factory via reflection");
      }
      
      if (!eeFactories.isEmpty()) {
        logger.debug("Successfully loaded {} EE MapReduce factories via reflection", eeFactories.size());
      }
      
    } catch (Exception e) {
      logger.warn("Failed to load EE MapReduce factories via reflection - continuing without EE MapReduce services", e);
    }
    
    return eeFactories;
  }

  /**
   * Loads Sqoop EE factories using reflection.
   * Returns properly typed factories for use with NamedClusterServiceLocator.
   * 
   * @param hadoopShim The Hadoop shim instance
   * @param authenticationMappingManager The authentication mapping manager
   * @return List of typed Sqoop service factories, or empty list if EE not available
   */
  public static List<NamedClusterServiceFactory<HadoopClientServices>> loadEESqoopFactories( 
      HadoopShim hadoopShim, 
      Object authenticationMappingManager ) {
    
    List<NamedClusterServiceFactory<HadoopClientServices>> factories = new ArrayList<>();
    
    if ( !isEEAvailable() ) {
      return factories;
    }
    
    try {
      // Load the EE Impersonating Hadoop Client Services Factory class
      Class<?> factoryClass = Class.forName( IMPERSONATING_HADOOP_CLIENT_SERVICES_FACTORY );
      
      // Get constructor with HadoopShim and AuthenticationMappingManager parameters
      Constructor<?> constructor = factoryClass.getConstructor( 
        HadoopShim.class, 
        Class.forName( "org.pentaho.authentication.mapper.api.AuthenticationMappingManager" ) 
      );
      
      // Create instance using reflection
      Object factoryInstance = constructor.newInstance( hadoopShim, authenticationMappingManager );
      
      // Cast to NamedClusterServiceFactory<HadoopClientServices> for proper type safety
      @SuppressWarnings("unchecked")
      NamedClusterServiceFactory<HadoopClientServices> typedFactory = 
        (NamedClusterServiceFactory<HadoopClientServices>) factoryInstance;
      
      factories.add( typedFactory );
      
      logger.debug( "Successfully loaded EE Sqoop factory: " + IMPERSONATING_HADOOP_CLIENT_SERVICES_FACTORY );
      
    } catch ( ClassNotFoundException e ) {
      logger.debug( "EE Sqoop factory class not found: " + IMPERSONATING_HADOOP_CLIENT_SERVICES_FACTORY );
    } catch ( NoSuchMethodException e ) {
      logger.warn( "EE Sqoop factory constructor signature mismatch for: " + IMPERSONATING_HADOOP_CLIENT_SERVICES_FACTORY, e );
    } catch ( Exception e ) {
      logger.error( "Failed to load EE Sqoop factory: " + IMPERSONATING_HADOOP_CLIENT_SERVICES_FACTORY, e );
    }
    
    return factories;
  }

  /**
   * Load EE HBase service factories using reflection
   * Creates impersonating and Knox HBase service factories when EE is available
   */
  public static List<NamedClusterServiceFactory<?>> loadEEHBaseFactories(
    Object hBaseShim,
    List<String> availableHbaseOptions, 
    Object authenticationMappingManager ) {
    
    List<NamedClusterServiceFactory<?>> eeFactories = new ArrayList<>();
    
    if ( !isEEAvailable() ) {
      logger.debug( "EE not available - skipping EE HBase factory creation" );
      return eeFactories;
    }
    
    try {
      // Load EE HBase impersonation factory
      if ( availableHbaseOptions.contains( "hbase_impersonation" ) && authenticationMappingManager != null ) {
        logger.debug( "Loading 'hbase_impersonation' factory via reflection" );
        Class<?> hBaseImpersonationServiceFactoryClass = Class.forName( "com.pentaho.hadoop.shim.hbase.security.impersonation.HBaseImpersonationServiceFactory" );
        
        Constructor<?> impersonationConstructor = hBaseImpersonationServiceFactoryClass.getDeclaredConstructor(
          Class.forName( "org.pentaho.hadoop.shim.spi.HBaseShim" ),
          Class.forName( "org.pentaho.authentication.mapper.api.AuthenticationMappingManager" )
        );
        
        Object hBaseImpersonationServiceFactory = impersonationConstructor.newInstance(
          hBaseShim,
          authenticationMappingManager
        );
        
        eeFactories.add( (NamedClusterServiceFactory<?>) hBaseImpersonationServiceFactory );
        logger.debug( "Successfully created hbase_impersonation factory via reflection" );
      } else if ( availableHbaseOptions.contains( "hbase_impersonation" ) ) {
        logger.debug( "Skipping hbase_impersonation factory - authentication manager not available" );
      }
      
      // Load EE HBase Knox factory
      if ( availableHbaseOptions.contains( "hbase_knox" ) ) {
        logger.debug( "Loading 'hbase_knox' factory via reflection" );
        Class<?> hBaseKnoxServiceFactoryClass = Class.forName( "com.pentaho.hadoop.shim.hbase.security.knox.HBaseKnoxServiceFactory" );
        
        Constructor<?> knoxConstructor = hBaseKnoxServiceFactoryClass.getDeclaredConstructor(
          Class.forName( "org.pentaho.hadoop.shim.spi.HBaseShim" )
        );
        
        Object hBaseKnoxServiceFactory = knoxConstructor.newInstance(
          hBaseShim
        );
        
        eeFactories.add( (NamedClusterServiceFactory<?>) hBaseKnoxServiceFactory );
        logger.debug( "Successfully created hbase_knox factory via reflection" );
      }
      
    } catch ( ClassNotFoundException e ) {
      logger.debug( "EE HBase factory classes not available on classpath - running in CE mode" );
    } catch ( Exception e ) {
      logger.error( "Failed to load EE HBase factories via reflection", e );
    }
    
    return eeFactories;
  }

  /**
   * Load EE Hive driver services using reflection
   * Creates impersonating Hive drivers for various Hive services when EE is available
   */
  public static List<Object> loadEEHiveDrivers(
    HadoopShim hadoopShim,
    List<String> availableHiveDrivers, 
    Object jdbcUrlParser,
    Object authenticationMappingManager ) {
    
    List<Object> eeDrivers = new ArrayList<>();
    
    if ( !isEEAvailable() || authenticationMappingManager == null ) {
      logger.debug( "EE not available or authentication manager is null - skipping EE Hive driver creation" );
      return eeDrivers;
    }
    
    try {
      // Load EE Hive impersonation drivers
      
      // Standard Hive impersonation driver
      if ( availableHiveDrivers.contains( "hive_impersonation" ) ) {
        logger.debug( "Loading 'hive_impersonation' driver via reflection" );
        Class<?> impersonatingHiveDriverClass = Class.forName( "com.pentaho.hadoop.shim.hive.security.impersonation.ImpersonatingHiveDriver" );
        
        Constructor<?> hiveConstructor = impersonatingHiveDriverClass.getDeclaredConstructor(
          String.class, // JDBC driver class name
          String.class, // Shim identifier
          jdbcUrlParser.getClass(), // JdbcUrlParser
          Class.forName( "org.pentaho.authentication.mapper.api.AuthenticationMappingManager" )
        );
        
        Object impersonatingHiveDriver = hiveConstructor.newInstance(
          "org.apache.hive.jdbc.HiveDriver",
          hadoopShim.getShimIdentifier().getId(),
          jdbcUrlParser,
          authenticationMappingManager
        );
        
        eeDrivers.add( impersonatingHiveDriver );
        logger.debug( "Successfully created hive_impersonation driver via reflection" );
      }
      
      // Impala impersonation driver
      if ( availableHiveDrivers.contains( "impala_impersonation" ) ) {
        logger.debug( "Loading 'impala_impersonation' driver via reflection" );
        Class<?> impersonatingImpalaDriverClass = Class.forName( "com.pentaho.hadoop.shim.hive.security.impersonation.ImpersonatingImpalaDriver" );
        
        Constructor<?> impalaConstructor = impersonatingImpalaDriverClass.getDeclaredConstructor(
          String.class,
          String.class,
          jdbcUrlParser.getClass(),
          Class.forName( "org.pentaho.authentication.mapper.api.AuthenticationMappingManager" )
        );
        
        Object impersonatingImpalaDriver = impalaConstructor.newInstance(
          "com.cloudera.impala.jdbc41.Driver",
          hadoopShim.getShimIdentifier().getId(),
          jdbcUrlParser,
          authenticationMappingManager
        );
        
        eeDrivers.add( impersonatingImpalaDriver );
        logger.debug( "Successfully created impala_impersonation driver via reflection" );
      }
      
      // Simba Hive impersonation driver
      if ( availableHiveDrivers.contains( "simba_impersonation" ) ) {
        logger.debug( "Loading 'simba_impersonation' driver via reflection" );
        Class<?> impersonatingHiveSimbaDriverClass = Class.forName( "com.pentaho.hadoop.shim.hive.security.impersonation.ImpersonatingHiveSimbaDriver" );
        
        Constructor<?> simbaConstructor = impersonatingHiveSimbaDriverClass.getDeclaredConstructor(
          String.class,
          String.class,
          jdbcUrlParser.getClass(),
          Class.forName( "org.pentaho.authentication.mapper.api.AuthenticationMappingManager" )
        );
        
        Object impersonatingHiveSimbaDriver = simbaConstructor.newInstance(
          "com.cloudera.impala.jdbc41.Driver",
          hadoopShim.getShimIdentifier().getId(),
          jdbcUrlParser,
          authenticationMappingManager
        );
        
        eeDrivers.add( impersonatingHiveSimbaDriver );
        logger.debug( "Successfully created simba_impersonation driver via reflection" );
      }
      
      // Impala Simba impersonation driver  
      if ( availableHiveDrivers.contains( "impala_simba_impersonation" ) ) {
        logger.debug( "Loading 'impala_simba_impersonation' driver via reflection" );
        Class<?> impersonatingImpalaSimbaDriverClass = Class.forName( "com.pentaho.hadoop.shim.hive.security.impersonation.ImpersonatingImpalaSimbaDriver" );
        
        Constructor<?> impalaSimbaConstructor = impersonatingImpalaSimbaDriverClass.getDeclaredConstructor(
          String.class,
          String.class,
          jdbcUrlParser.getClass(),
          Class.forName( "org.pentaho.authentication.mapper.api.AuthenticationMappingManager" )
        );
        
        Object impersonatingImpalaSimbaDriver = impalaSimbaConstructor.newInstance(
          "com.cloudera.impala.jdbc41.Driver",
          hadoopShim.getShimIdentifier().getId(),
          jdbcUrlParser,
          authenticationMappingManager
        );
        
        eeDrivers.add( impersonatingImpalaSimbaDriver );
        logger.debug( "Successfully created impala_simba_impersonation driver via reflection" );
      }
      
      // Spark Simba impersonation driver
      if ( availableHiveDrivers.contains( "spark_simba_impersonation" ) ) {
        logger.debug( "Loading 'spark_simba_impersonation' driver via reflection" );
        Class<?> impersonatingSparkSqlSimbaDriverClass = Class.forName( "com.pentaho.hadoop.shim.hive.security.impersonation.ImpersonatingSparkSqlSimbaDriver" );
        
        Constructor<?> sparkSimbaConstructor = impersonatingSparkSqlSimbaDriverClass.getDeclaredConstructor(
          String.class,
          String.class,
          jdbcUrlParser.getClass(),
          Class.forName( "org.pentaho.authentication.mapper.api.AuthenticationMappingManager" )
        );
        
        Object impersonatingSparkSqlSimbaDriver = sparkSimbaConstructor.newInstance(
          "org.apache.hive.jdbc.HiveDriver",
          hadoopShim.getShimIdentifier().getId(),
          jdbcUrlParser,
          authenticationMappingManager
        );
        
        eeDrivers.add( impersonatingSparkSqlSimbaDriver );
        logger.debug( "Successfully created spark_simba_impersonation driver via reflection" );
      }
      
    } catch ( ClassNotFoundException e ) {
      logger.debug( "EE Hive driver classes not available on classpath - running in CE mode" );
    } catch ( Exception e ) {
      logger.error( "Failed to load EE Hive drivers via reflection", e );
    }
    
    return eeDrivers;
  }

  /**
   * Load EE Yarn service factory via reflection
   * 
   * @param hadoopFileSystemLocator The file system locator for Yarn services
   * @return NamedClusterServiceFactory representing the YarnServiceFactoryImpl, or null if EE classes not available
   */
  public static NamedClusterServiceFactory<?> loadEEYarnServiceFactory( Object hadoopFileSystemLocator, Object authenticationMappingManager ) {
    if ( !isEEAvailable() ) {
      logger.debug( "EE services not available - skipping Yarn service factory loading" );
      return null;
    }
    
    logger.debug( "Loading EE Yarn service factory via reflection" );
    
    try {
      // Load the EE Yarn service factory class
      Class<?> yarnServiceFactoryClass = Class.forName( "com.pentaho.yarn.impl.shim.YarnServiceFactoryImpl" );
      
      // Create instance using constructor: YarnServiceFactoryImpl(HadoopFileSystemLocator hadoopFileSystemLocator)
      Constructor<?> constructor = yarnServiceFactoryClass.getConstructor( 
        Class.forName( "org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator" ),
        Class.forName( "org.pentaho.authentication.mapper.api.AuthenticationMappingManager" )
      );
      Object yarnServiceFactory = constructor.newInstance( hadoopFileSystemLocator ,authenticationMappingManager );
      
      logger.debug( "Successfully loaded EE Yarn service factory: " + yarnServiceFactory.getClass().getSimpleName() );
      
      // Cast to NamedClusterServiceFactory for proper type safety
      return (NamedClusterServiceFactory<?>) yarnServiceFactory;
      
    } catch ( ClassNotFoundException e ) {
      logger.debug( "EE Yarn service factory class not found - continuing in CE mode" );
      return null;
    } catch ( NoSuchMethodException e ) {
      logger.warn( "EE Yarn service factory constructor not found", e );
      return null;
    } catch ( Exception e ) {
      logger.warn( "Failed to load EE Yarn service factory", e );
      return null;
    }
  }
}
