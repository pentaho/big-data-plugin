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


package org.pentaho.s3common;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.pentaho.amazon.s3.S3Util;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.util.StorageUnitConverter;
import org.pentaho.di.i18n.BaseMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

public abstract class S3CommonFileSystem extends AbstractFileSystem {

  private static final Class<?> PKG = S3CommonFileSystem.class;
  private static final Logger logger = LoggerFactory.getLogger( PKG );
  private static final String DEFAULT_S3_CONFIG_PROPERTY = "defaultS3Config";

  // S3 part size constants
  /**
   * Minimum part size specified in documentation
   * see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
   */
  private static final String MIN_PART_SIZE = "5MB";
  /**
   * Maximum part size specified in documentation
   * see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
   */
  private static final String MAX_PART_SIZE = "5GB";
  private static final StorageUnitConverter STATIC_STORAGE_UNIT_CONVERTER = new StorageUnitConverter();
  private static final long MIN_PART_SIZE_BYTES = STATIC_STORAGE_UNIT_CONVERTER.displaySizeToByteCount( MIN_PART_SIZE );
  private static final long MAX_PART_SIZE_BYTES = STATIC_STORAGE_UNIT_CONVERTER.displaySizeToByteCount( MAX_PART_SIZE );

  // S3 client and connection state
  private AmazonS3 client;
  private String awsAccessKeyCache;
  private String awsSecretKeyCache;
  private FileSystemOptions currentFileSystemOptions;
  private Map<String, String> currentConnectionProperties;
  private final Supplier<ConnectionManager> connectionManager = ConnectionManager::getInstance;

  // Helpers and utilities
  protected StorageUnitConverter storageUnitConverter;
  protected S3KettleProperty s3KettleProperty;
  protected S3TransferManager s3TransferManager;

  // Short-lived cache of metadata already returned by a folder listing (ListObjects), so that resolving/attaching
  // those same children right afterward (as Commons-VFS traversal does when e.g. the "Get File Names" step lists an
  // S3 folder) can reuse it instead of issuing a redundant per-file getObjectMetadata (HEAD) request for every
  // listed object.
  private static final int LISTING_CACHE_MAX_ENTRIES = 50_000;

  // Explicit on/off switch (S3KettleProperty#S3VFS_CACHE_ENABLED, default true) and TTL
  // (S3KettleProperty#S3VFS_CACHE_TTL_SECONDS, default 60s) for both caches below. When disabled, every
  // lookup skips the cache entirely (no reads, no writes) and always falls through to a live S3 check -
  // useful for deployments where external processes may mutate the same buckets/objects concurrently
  // and can't tolerate any staleness.
  private final boolean cacheEnabled;
  private final long cacheTtlNanos;
  private final AtomicLong lastEvictionNanos = new AtomicLong( System.nanoTime() );

  private final Map<String, CachedListingEntry> listingMetadataCache =
    Collections.synchronizedMap( new LinkedHashMap<>( 16, 0.75f, false ) {
      @Override
      protected boolean removeEldestEntry( Map.Entry<String, CachedListingEntry> eldest ) {
        return size() > LISTING_CACHE_MAX_ENTRIES;
      }
    } );

  // Short-lived cache of bucket-existence checks. The legacy S3FileObject.fixFilePath()/bucketExists()
  // path calls AmazonS3#doesBucketExistV2() (a real network round-trip, potentially with a fresh TLS
  // handshake) on essentially every VFS operation (isRootBucket(), getS3Object(), doDelete(), etc.),
  // which previously meant one such network call was made per file when traversing/attaching every child
  // of a listed folder (e.g. "Get File Names"). Bucket existence effectively never changes during a
  // single listing/traversal operation, so caching it briefly avoids this large, unnecessary per-file
  // network overhead while still re-checking periodically in case the bucket really is removed.
  // Bounded (LRU, evicting the least recently used bucket) so a workload touching many bucket names can't grow it
  // without limit within a TTL window.
  private static final int BUCKET_CACHE_MAX_ENTRIES = 1_000;
  private final Map<String, CachedBucketExists> bucketExistsCache =
    Collections.synchronizedMap( new LinkedHashMap<>( 16, 0.75f, true ) {
      @Override
      protected boolean removeEldestEntry( Map.Entry<String, CachedBucketExists> eldest ) {
        return size() > BUCKET_CACHE_MAX_ENTRIES;
      }
    } );

  // Bumped (under cacheLock) every time the caches are invalidated because the client/connection changed. Cache
  // fills remember the generation they started under and are dropped if it changed, so a lookup that began against
  // an old client can't publish its result after the invalidation.
  private final Object cacheLock = new Object();
  // Environment credentials the caches were filled under (tracked apart from the client's own snapshot).
  private String cachedEnvAccessKey = System.getProperty( S3Util.ACCESS_KEY_SYSTEM_PROPERTY );
  private String cachedEnvSecretKey = System.getProperty( S3Util.SECRET_KEY_SYSTEM_PROPERTY );
  private volatile long cacheGeneration;

  /** @return the current cache generation; capture it before a live S3 call and pass it to the cache fill. */
  public long getCacheGeneration() {
    return cacheGeneration;
  }

  // Striped locks guarding the per-bucket check-and-publish below. A fixed number of monitors keeps memory
  // independent of how many distinct bucket names are seen; buckets that share a stripe only serialize their checks.
  private static final int BUCKET_LOCK_STRIPES = 32;
  private final Object[] bucketLocks = createBucketLocks();

  private static Object[] createBucketLocks() {
    Object[] locks = new Object[ BUCKET_LOCK_STRIPES ];

    for ( int i = 0; i < locks.length; i++ ) {
      locks[ i ] = new Object();
    }

    return locks;
  }

  private Object bucketLock( String bucketName ) {
    return bucketLocks[ Math.floorMod( bucketName.hashCode(), BUCKET_LOCK_STRIPES ) ];
  }

  public boolean isBucketExists( String bucketName, Predicate<String> bucketExistsCheck ) {
    if ( !cacheEnabled ) {
      return bucketExistsCheck.test( bucketName );
    }

    validateCacheAgainstClientConfiguration();

    CachedBucketExists cached = getValidBucketEntry( bucketName );

    if ( cached != null ) {
      return cached.exists;
    }

    // Refresh atomically per bucket: concurrent callers wait for a single live check instead of each issuing one,
    // and an older result can never overwrite a newer one. If the check throws, nothing is cached.
    synchronized ( bucketLock( bucketName ) ) {
      cached = getValidBucketEntry( bucketName );

      if ( cached != null ) {
        return cached.exists;
      }

      long generation = cacheGeneration;
      try {
        boolean exists = bucketExistsCheck.test( bucketName );
        synchronized ( cacheLock ) {
          if ( generation == cacheGeneration ) {
            bucketExistsCache.put( bucketName, new CachedBucketExists( exists, cacheTtlNanos ) );
          }
        }
        return exists;
      } finally {
        // Also sweep after failed checks, so expired entries don't linger while checks keep failing.
        evictExpiredIfDue();
      }
    }
  }

  /**
   * Reads and validates under {@code cacheLock}, the same lock invalidation holds, so an entry is never returned
   * once it has been invalidated.
   */
  private CachedBucketExists getValidBucketEntry( String bucketName ) {
    synchronized ( cacheLock ) {
      CachedBucketExists cached = bucketExistsCache.get( bucketName );
      return cached != null && !cached.isExpired() ? cached : null;
    }
  }

  private static final class CachedBucketExists {
    private final boolean exists;
    private final long expiresAtNanos;

    private CachedBucketExists( boolean exists, long ttlNanos ) {
      this.exists = exists;
      this.expiresAtNanos = System.nanoTime() + ttlNanos;
    }

    private boolean isExpired() {
      return System.nanoTime() > expiresAtNanos;
    }
  }

  protected S3CommonFileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions ) {
    this( rootName, fileSystemOptions, STATIC_STORAGE_UNIT_CONVERTER, new S3KettleProperty() );
  }

  protected S3CommonFileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions,
                                final StorageUnitConverter storageUnitConverter, final S3KettleProperty s3KettleProperty ) {
    super( rootName, null, fileSystemOptions );
    this.storageUnitConverter = storageUnitConverter;
    this.s3KettleProperty = s3KettleProperty;
    this.currentConnectionProperties = new HashMap<>();
    this.cacheEnabled = s3KettleProperty.isCacheEnabled();
    this.cacheTtlNanos = TimeUnit.SECONDS.toNanos( s3KettleProperty.getCacheTtlSeconds() );
    applyEc2MetadataSetting( s3KettleProperty );
  }

  /** Honors s3.vfs.disableEc2Metadata without overriding an explicit JVM-level setting. */
  private static void applyEc2MetadataSetting( S3KettleProperty s3KettleProperty ) {
    if ( s3KettleProperty.isDisableEc2Metadata()
      && System.getProperty( SDKGlobalConfiguration.AWS_EC2_METADATA_DISABLED_SYSTEM_PROPERTY ) == null ) {
      System.setProperty( SDKGlobalConfiguration.AWS_EC2_METADATA_DISABLED_SYSTEM_PROPERTY, Boolean.TRUE.toString() );
    }
  }

  @Override
  protected void addCapabilities( Collection<Capability> caps ) {
    caps.addAll( S3CommonFileProvider.capabilities );
  }

  protected abstract FileObject createFile( AbstractFileName name ) throws Exception;

  public AmazonS3 getS3Client() {
    S3CommonFileSystemConfigBuilder s3CommonFileSystemConfigBuilder =
      new S3CommonFileSystemConfigBuilder( getFileSystemOptions() );

    Optional<? extends ConnectionDetails> defaultS3Connection =
      getDefaultS3Connection( connectionManager, s3CommonFileSystemConfigBuilder );

    clearClientAndCacheIfChanged( s3CommonFileSystemConfigBuilder, defaultS3Connection );

    if ( client == null && getFileSystemOptions() != null ) {
      currentFileSystemOptions = getFileSystemOptions();
      String accessKey = null;
      String secretKey = null;
      String sessionToken = null;
      String region = null;
      String credentialsFilePath = null;
      String profileName = null;
      String endpoint = null;
      String signatureVersion = null;
      String pathStyleAccess = null;

      if ( s3CommonFileSystemConfigBuilder.getName() == null && defaultS3Connection.isPresent() ) {
        accessKey = Encr.decryptPasswordOptionallyEncrypted( currentConnectionProperties.get( "accessKey" ) );
        secretKey = Encr.decryptPasswordOptionallyEncrypted( currentConnectionProperties.get( "secretKey" ) );
        sessionToken = Encr.decryptPasswordOptionallyEncrypted( currentConnectionProperties.get( "sessionToken" ) );
        region = currentConnectionProperties.get( "region" );
        credentialsFilePath = currentConnectionProperties.get( "credentialsFilePath" );
        profileName = currentConnectionProperties.get( "profileName" );
        endpoint = currentConnectionProperties.get( "endpoint" );
        signatureVersion = currentConnectionProperties.get( "signatureVersion" );
        pathStyleAccess = currentConnectionProperties.get( "pathStyleAccess" );
      } else {
        accessKey = s3CommonFileSystemConfigBuilder.getAccessKey();
        secretKey = s3CommonFileSystemConfigBuilder.getSecretKey();
        sessionToken = s3CommonFileSystemConfigBuilder.getSessionToken();
        region = s3CommonFileSystemConfigBuilder.getRegion();
        credentialsFilePath = s3CommonFileSystemConfigBuilder.getCredentialsFile();
        profileName = s3CommonFileSystemConfigBuilder.getProfileName();
        endpoint = s3CommonFileSystemConfigBuilder.getEndpoint();
        signatureVersion = s3CommonFileSystemConfigBuilder.getSignatureVersion();
        pathStyleAccess = s3CommonFileSystemConfigBuilder.getPathStyleAccess();
      }
      boolean access = ( pathStyleAccess == null ) || Boolean.parseBoolean( pathStyleAccess );

      AWSCredentialsProvider awsCredentialsProvider = null;
      Regions regions = Regions.DEFAULT_REGION;

      S3Util.S3Keys keys = S3Util.getKeysFromURI( getRootURI() );
      if ( keys != null ) {
        accessKey = keys.getAccessKey();
        secretKey = keys.getSecretKey();
      }

      if ( !S3Util.isEmpty( accessKey ) && !S3Util.isEmpty( secretKey ) ) {
        AWSCredentials awsCredentials;
        if ( S3Util.isEmpty( sessionToken ) ) {
          awsCredentials = new BasicAWSCredentials( accessKey, secretKey );
        } else {
          awsCredentials = new BasicSessionCredentials( accessKey, secretKey, sessionToken );
        }
        awsCredentialsProvider = new AWSStaticCredentialsProvider( awsCredentials );
        regions = S3Util.isEmpty( region ) ? Regions.DEFAULT_REGION : Regions.fromName( region );
      } else if ( !S3Util.isEmpty( credentialsFilePath ) ) {
        ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile( credentialsFilePath );
        awsCredentialsProvider = new ProfileCredentialsProvider( profilesConfigFile, profileName );
      }

      if ( !S3Util.isEmpty( endpoint ) ) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride(
          S3Util.isEmpty( signatureVersion ) ? S3Util.SIGNATURE_VERSION_SYSTEM_PROPERTY : signatureVersion );
        client = AmazonS3ClientBuilder.standard()
          .withEndpointConfiguration( new AwsClientBuilder.EndpointConfiguration( endpoint, regions.getName() ) )
          .withPathStyleAccessEnabled( access )
          .withClientConfiguration( clientConfiguration )
          .withCredentials( awsCredentialsProvider )
          .build();
      } else {
        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard()
          .enableForceGlobalBucketAccess()
          .withCredentials( awsCredentialsProvider );
        if ( !isRegionSet() ) {
          clientBuilder.withRegion( regions );
        }
        client = clientBuilder.build();
      }
    }

    if ( client == null || hasClientChangedCredentials() ) {
      // Nothing can be cached for a client that never existed; only a replaced client invalidates caches.
      if ( client != null ) {
        clearMetadataCaches();
      }

      try {
        if ( isRegionSet() ) {
          client = AmazonS3ClientBuilder.standard()
            .enableForceGlobalBucketAccess()
            .build();
        } else {
          client = AmazonS3ClientBuilder.standard()
            .enableForceGlobalBucketAccess()
            .withRegion( Regions.DEFAULT_REGION )
            .build();
        }
        awsAccessKeyCache = System.getProperty( S3Util.ACCESS_KEY_SYSTEM_PROPERTY );
        awsSecretKeyCache = System.getProperty( S3Util.SECRET_KEY_SYSTEM_PROPERTY );
        // Keep the cache-validation snapshot in step with the client just built, so listings cached under it survive.
        synchronized ( cacheLock ) {
          cachedEnvAccessKey = awsAccessKeyCache;
          cachedEnvSecretKey = awsSecretKeyCache;
        }
      } catch ( Exception ex ) {
        logger.error( "Could not get an S3Client", ex );
      }
    }
    return client;
  }

  private static Optional<? extends ConnectionDetails> getDefaultS3Connection(
    Supplier<ConnectionManager> connectionManagerSupplier,
    S3CommonFileSystemConfigBuilder s3CommonFileSystemConfigBuilder ) {
    Optional<? extends ConnectionDetails> defaultS3Connection = Optional.empty();
    try {
      if ( s3CommonFileSystemConfigBuilder.useDefaults() ) {
        defaultS3Connection =
        connectionManagerSupplier.get().getConnectionDetailsByScheme( "s3" ).stream().filter(
          connectionDetails -> connectionDetails.getProperties().get( DEFAULT_S3_CONFIG_PROPERTY ) != null
            && connectionDetails.getProperties().get( DEFAULT_S3_CONFIG_PROPERTY ).equalsIgnoreCase( "true" ) )
        .findFirst();
      }
    } catch ( Exception ignored ) {
      // Ignore the exception, it's OK if we can't find a default S3 connection.
    }
    return defaultS3Connection;
  }

  private void clearClientAndCacheIfChanged( S3CommonFileSystemConfigBuilder s3CommonFileSystemConfigBuilder,
                                             Optional<? extends ConnectionDetails> defaultS3Connection ) {
    // If the fileSystemOptions don't contain a name, the originating url is s3:// NOT pvfs://
    // Use a specified default PVFS connection if it's available.
    if ( s3CommonFileSystemConfigBuilder.getName() == null ) {
      // Copy the connection properties
      Map<String, String> newConnectionProperties = new HashMap<>();
      defaultS3Connection
              .ifPresent( connectionDetails -> newConnectionProperties.putAll( connectionDetails.getProperties() ) );

      // Have the default connection properties changed?
      if ( !newConnectionProperties.equals( currentConnectionProperties ) ) {
        // Force a new connection if the default PVFS was changed
        client = null;
        // Track the new connection
        currentConnectionProperties = newConnectionProperties;
        // Clear the file system cache as the credentials have changed and the cache is now invalid.
        this.getFileSystemManager().getFilesCache().clear( this );
        clearMetadataCaches();
      }
    }

    if ( currentFileSystemOptions != null && !currentFileSystemOptions.equals( getFileSystemOptions() ) ) {
      client = null;
      currentFileSystemOptions = getFileSystemOptions();
      this.getFileSystemManager().getFilesCache().clear( this );
      clearMetadataCaches();
    }
  }

  /**
   * Cache hits bypass {@link #getS3Client()}, which is where connection changes are normally detected. Run the same
   * detection (filesystem options, default connection, environment credentials) before serving a hit, so cached
   * metadata from a previous connection is dropped. This never creates a client or contacts S3.
   */
  private void validateCacheAgainstClientConfiguration() {
    S3CommonFileSystemConfigBuilder configBuilder = new S3CommonFileSystemConfigBuilder( getFileSystemOptions() );
    clearClientAndCacheIfChanged( configBuilder, getDefaultS3Connection( connectionManager, configBuilder ) );

    String accessKey = System.getProperty( S3Util.ACCESS_KEY_SYSTEM_PROPERTY );
    String secretKey = System.getProperty( S3Util.SECRET_KEY_SYSTEM_PROPERTY );
    boolean credentialsChanged;

    synchronized ( cacheLock ) {
      credentialsChanged = S3Util.hasChanged( cachedEnvAccessKey, accessKey )
        || S3Util.hasChanged( cachedEnvSecretKey, secretKey );
      cachedEnvAccessKey = accessKey;
      cachedEnvSecretKey = secretKey;

      if ( credentialsChanged ) {
        clearMetadataCaches();
      }
    }
  }

  /** Drop bucket-existence and listing metadata cached for a previous client/connection. */
  private void clearMetadataCaches() {
    synchronized ( cacheLock ) {
      cacheGeneration++;
      bucketExistsCache.clear();
      listingMetadataCache.clear();
    }
  }

  /**
   * Expired entries are otherwise only dropped when their own key is read again, so sweep both caches on write, at
   * most once per TTL period, to release entries that are never looked up again.
   */
  private void evictExpiredIfDue() {
    long now = System.nanoTime();
    long last = lastEvictionNanos.get();

    if ( now - last < cacheTtlNanos || !lastEvictionNanos.compareAndSet( last, now ) ) {
      return;
    }

    synchronized ( bucketExistsCache ) {
      bucketExistsCache.values().removeIf( CachedBucketExists::isExpired );
    }

    synchronized ( listingMetadataCache ) {
      listingMetadataCache.values().removeIf( CachedListingEntry::isExpired );
    }
  }

  private boolean hasClientChangedCredentials() {
    return client != null
      && ( S3Util.hasChanged( awsAccessKeyCache, System.getProperty( S3Util.ACCESS_KEY_SYSTEM_PROPERTY ) )
      || S3Util.hasChanged( awsSecretKeyCache, System.getProperty( S3Util.SECRET_KEY_SYSTEM_PROPERTY ) ) );
  }

  protected boolean isRegionSet() {
    //region is set if explicitly set in env variable or configuration file is explicitly set
    if ( System.getenv( S3Util.AWS_REGION ) != null || System.getenv( S3Util.AWS_CONFIG_FILE ) != null ) {
      return true;
    }
    //check if configuration file exists in default location
    File awsConfigFolder = new File(
      System.getProperty( "user.home" ) + File.separator + S3Util.AWS_FOLDER + File.separator + S3Util.CONFIG_FILE );
    if ( awsConfigFolder.exists() ) {
      return true;
    }
    //When running on an Amazon EC2 instance getCurrentRegion will get its region. Null if not running in an EC2 instance
    return Regions.getCurrentRegion() != null;
  }

  public long getPartSize() {
    return parsePartSize( s3KettleProperty.getPartSize() );
  }

  protected long parsePartSize( String partSizeString ) {
    long parsePartSize = convertToLong( partSizeString );
    if ( parsePartSize < MIN_PART_SIZE_BYTES ) {
      if ( logger.isWarnEnabled() ) {
        logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.DefaultPartSize", partSizeString, MIN_PART_SIZE ) );
      }
      parsePartSize = MIN_PART_SIZE_BYTES;
    }

    // still allow > 5GB, api might be updated in the future
    if ( logger.isWarnEnabled() && parsePartSize > MAX_PART_SIZE_BYTES ) {
      logger.warn( BaseMessages.getString( PKG, "WARN.S3MultiPart.MaximumPartSize", partSizeString, MAX_PART_SIZE ) );
    }
    return parsePartSize;
  }

  protected long convertToLong( String partSize ) {
    return storageUnitConverter.displaySizeToByteCount( partSize );
  }

  protected TransferManager buildTransferManager() {
    TransferManagerBuilder transferManagerBuilder = TransferManagerBuilder.standard()
      .withS3Client( getS3Client() );
    if ( getPartSize() > 0 ) {
      transferManagerBuilder.withMinimumUploadPartSize( getPartSize() );
      transferManagerBuilder.withMultipartCopyPartSize( getPartSize() ); // Only used above 5GB by default, can be changed with multipartCopyThreshold
    }
    return transferManagerBuilder.build();
  }

  protected S3TransferManager getS3TransferManager() {
    if ( s3TransferManager == null ) {
      s3TransferManager = new S3TransferManager( buildTransferManager() );
    }
    return s3TransferManager;
  }

  /**
   * Uploads the content of the specified FileObject to the specified S3FileObject using multipart upload.
   *
   * @param src The source FileObject to upload from.
   * @param dst The destination S3FileObject to upload to.
   * @throws FileSystemException If an error occurs during the upload operation.
   */
  public void upload( FileObject src, S3CommonFileObject dst ) throws FileSystemException {
    getS3TransferManager().upload( src, dst );
  }

  /**
   * Copies the content of the specified S3FileObject to another S3FileObject using server-side multipart copy.
   * If the source or destination is not an S3FileObject, it will throw a FileSystemException.
   *
   * @param src  The source S3FileObject to copy from.
   * @param dest The destination S3FileObject to copy to.
   * @throws FileSystemException If an error occurs during the copy operation.
   */
  public void copy( S3CommonFileObject src, S3CommonFileObject dest ) throws FileSystemException {
    getS3TransferManager().copy( src, dest );
  }

  /**
   * Cache the metadata for a file found while listing its parent folder, so that resolving/attaching this same file
   * right afterward can reuse it instead of issuing a redundant per-file {@code getObjectMetadata} request. See
   * {@link #getCachedFileMetadata}.
   */
  public void cacheListedFile( String bucketName, S3ObjectSummary summary ) {
    cacheListedFile( bucketName, summary, cacheGeneration );
  }

  /**
   * @param generation the {@link #getCacheGeneration()} captured before the listing call that produced
   *                   {@code summary}; the entry is dropped if the caches were invalidated since then
   */
  public void cacheListedFile( String bucketName, S3ObjectSummary summary, long generation ) {
    if ( !cacheEnabled ) {
      return;
    }

    // A summary ending in the delimiter is a folder marker object, so it must be cached as a folder.
    if ( summary.getKey().endsWith( S3CommonFileObject.DELIMITER ) ) {
      cacheListedFolder( bucketName, summary.getKey(), generation );
      return;
    }

    putListingEntry( listingCacheKey( bucketName, summary.getKey() ),
      new CachedListingEntry( FileType.FILE, summary.getSize(), summary.getLastModified(), cacheTtlNanos ),
      generation );
  }

  private void putListingEntry( String cacheKey, CachedListingEntry entry, long generation ) {
    synchronized ( cacheLock ) {
      if ( generation != cacheGeneration ) {
        return;
      }

      listingMetadataCache.put( cacheKey, entry );
    }

    evictExpiredIfDue();
  }

  /**
   * Cache a subfolder ("common prefix") found while listing its parent folder. See {@link #isCachedFolder}.
   *
   * @param keyWithDelimiter the subfolder's bucket-relative key, including its trailing delimiter
   */
  public void cacheListedFolder( String bucketName, String keyWithDelimiter ) {
    cacheListedFolder( bucketName, keyWithDelimiter, cacheGeneration );
  }

  /** @param generation see {@link #cacheListedFile(String, S3ObjectSummary, long)} */
  public void cacheListedFolder( String bucketName, String keyWithDelimiter, long generation ) {
    if ( !cacheEnabled ) {
      return;
    }

    putListingEntry( listingCacheKey( bucketName, keyWithDelimiter ),
      new CachedListingEntry( FileType.FOLDER, 0L, null, cacheTtlNanos ), generation );
  }

  /**
   * @return the cached {@link ObjectMetadata} for {@code key} if it was seen as a file in a recent-enough folder
   * listing, or {@code null} on a cache miss/expiry (meaning the caller must fall back to fetching it from S3).
   */
  public ObjectMetadata getCachedFileMetadata( String bucketName, String key ) {
    CachedListingEntry entry = getValidCachedEntry( bucketName, key );
    return entry != null && entry.type == FileType.FILE ? entry.toObjectMetadata() : null;
  }

  /**
   * @return whether {@code keyWithDelimiter} was seen as a subfolder in a recent-enough folder listing (meaning its
   * existence/type doesn't need to be re-checked against S3).
   */
  public boolean isCachedFolder( String bucketName, String keyWithDelimiter ) {
    CachedListingEntry entry = getValidCachedEntry( bucketName, keyWithDelimiter );
    return entry != null && entry.type == FileType.FOLDER;
  }

  /**
   * Drop any cached listing metadata for {@code key}, as both a file key and a folder key (with trailing
   * delimiter), plus every ancestor folder key (a listing can cache a virtual prefix such as {@code dir/} that
   * disappears when its last child is deleted or moved). To be called after this filesystem successfully creates,
   * overwrites, copies or deletes an object so a fresh attach doesn't serve stale metadata until the TTL expires.
   */
  public void invalidateCachedListing( String bucketName, String key ) {
    if ( !cacheEnabled || key == null ) {
      return;
    }

    String plainKey = withoutTrailingDelimiter( key );

    synchronized ( cacheLock ) {
      // Also bump the generation so a listing that started before this mutation can't re-cache the old state.
      cacheGeneration++;
      listingMetadataCache.remove( listingCacheKey( bucketName, plainKey ) );
      listingMetadataCache.remove( listingCacheKey( bucketName, plainKey + S3CommonFileObject.DELIMITER ) );

      int separator = plainKey.lastIndexOf( S3CommonFileObject.DELIMITER );
      while ( separator > 0 ) {
        String ancestor = plainKey.substring( 0, separator );
        listingMetadataCache.remove( listingCacheKey( bucketName, ancestor ) );
        listingMetadataCache.remove( listingCacheKey( bucketName, ancestor + S3CommonFileObject.DELIMITER ) );
        separator = ancestor.lastIndexOf( S3CommonFileObject.DELIMITER );
      }
    }
  }

  private static String withoutTrailingDelimiter( String key ) {
    return key.endsWith( S3CommonFileObject.DELIMITER )
      ? key.substring( 0, key.length() - S3CommonFileObject.DELIMITER.length() ) : key;
  }

  private CachedListingEntry getValidCachedEntry( String bucketName, String key ) {
    if ( !cacheEnabled ) {
      return null;
    }

    validateCacheAgainstClientConfiguration();

    String cacheKey = listingCacheKey( bucketName, key );
    // Lookup, validity decision and expiry removal share cacheLock with invalidation, so an invalidated entry is
    // never returned.
    synchronized ( cacheLock ) {
      CachedListingEntry entry = listingMetadataCache.get( cacheKey );

      if ( entry == null ) {
        return null;
      }

      if ( entry.isExpired() ) {
        listingMetadataCache.remove( cacheKey, entry );
        return null;
      }

      return entry;
    }
  }

  private static String listingCacheKey( String bucketName, String key ) {
    return bucketName + '\u0000' + key;
  }

  /**
   * Metadata captured for a single object/subfolder while listing its parent folder.
   */
  private static final class CachedListingEntry {
    private final FileType type;
    private final long contentLength;
    private final Date lastModified;
    private final long expiresAtNanos;

    private CachedListingEntry( FileType type, long contentLength, Date lastModified, long ttlNanos ) {
      this.type = type;
      this.contentLength = contentLength;
      this.lastModified = lastModified;
      this.expiresAtNanos = System.nanoTime() + ttlNanos;
    }

    private boolean isExpired() {
      return System.nanoTime() > expiresAtNanos;
    }

    private ObjectMetadata toObjectMetadata() {
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength( contentLength );

      if ( lastModified != null ) {
        metadata.setLastModified( lastModified );
      }

      return metadata;
    }
  }
}
