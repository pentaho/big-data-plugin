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

package org.pentaho.big.data.it;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * Downloads and unpacks a published Big Data plugin variant for the nightly integration test flow.
 * <p>
 * For PR/merge builds the plugin is taken from the local reactor (see the {@code local} source in the POM).
 * When running with {@code -Dbigdata.it.source=download}, this manager fetches the requested variant
 * (apache, cdp, dataproc, emr) and unzips it so it can be assembled into the PDI container, exactly like
 * the locally built plugin would be.
 * <p>
 * System properties:
 * <ul>
 *   <li>{@code bigdata.it.pdi-version} (required) - selects the default download base URL.</li>
 *   <li>{@code bigdata.it.plugin-variant} (default {@code apache}).</li>
 *   <li>{@code bigdata.it.plugin-unpack-dir} (required) - where the {@code pentaho-big-data-plugin} folder is created.</li>
 *   <li>{@code bigdata.it.cache-dir} (default {@code ${user.home}/.pentaho-big-data/it-cache}).</li>
 *   <li>{@code bigdata.it.plugin.url} (optional) - explicit ZIP URL override.</li>
 * </ul>
 */
public final class BigDataPluginCacheManager {

  private static final String PROP_PDI_VERSION = "bigdata.it.pdi-version";
  private static final String PROP_VARIANT = "bigdata.it.plugin-variant";
  private static final String PROP_UNPACK_DIR = "bigdata.it.plugin-unpack-dir";
  private static final String PROP_CACHE_DIR = "bigdata.it.cache-dir";
  private static final String PROP_URL_OVERRIDE = "bigdata.it.plugin.url";

  private static final String PLUGIN_DIR_NAME = "pentaho-big-data-plugin";
  private static final String ARTIFACTORY_HOST = "one.hitachivantara.com";

  /** Base download URL per PDI version. New versions are added here. */
  private static final Map<String, String> DEFAULT_BASE_URLS = new HashMap<>();

  static {
    // For 11.1 we intentionally track the latest QAT build.
    DEFAULT_BASE_URLS.put( "11.1", "https://build.eng.pentaho.com/hosted/11.1-QAT/latest/" );
  }

  private BigDataPluginCacheManager() {
  }

  public static void main( String[] args ) {
    try {
      String pdiVersion = required( PROP_PDI_VERSION );
      String variant = System.getProperty( PROP_VARIANT, "apache" ).trim();
      Path unpackDir = Path.of( required( PROP_UNPACK_DIR ) ).toAbsolutePath().normalize();
      Path cacheDir = Path.of( System.getProperty( PROP_CACHE_DIR,
          Path.of( System.getProperty( "user.home" ), ".pentaho-big-data", "it-cache" ).toString() ) )
        .toAbsolutePath().normalize();

      String url = resolveUrl( pdiVersion, variant );
      if ( url == null ) {
        System.out.println( "[INFO] No download URL for pdiVersion=" + pdiVersion + " variant=" + variant
          + ". Skipping download." );
        return;
      }

      Files.createDirectories( unpackDir );
      Files.createDirectories( cacheDir );

      URI artifactUri = URI.create( url );
      Path artifactName = Path.of( Objects.requireNonNull( artifactUri.getPath() ) ).getFileName();
      if ( artifactName == null || !artifactName.toString().endsWith( ".zip" ) ) {
        throw new IOException( "Resolved plugin URL does not point to a ZIP artifact: " + url );
      }
      Path zipFile = cacheDir.resolve( artifactName.toString() );

      // Moving "latest" artifacts must never be reused across runs.
      if ( url.contains( "/latest/" ) && Files.exists( zipFile ) ) {
        Files.delete( zipFile );
      }

      System.out.println( "[INFO] Big Data plugin download" );
      System.out.println( "[INFO] pdiVersion=" + pdiVersion + " variant=" + variant );
      System.out.println( "[INFO] url=" + url );
      System.out.println( "[INFO] cachedZip=" + zipFile );
      System.out.println( "[INFO] unpackDir=" + unpackDir );

      cleanDirectoryContents( unpackDir );
      downloadIfMissing( artifactUri, zipFile );
      unzip( zipFile, unpackDir );

      Path finalPluginDir = unpackDir.resolve( PLUGIN_DIR_NAME );
      if ( !Files.isDirectory( finalPluginDir ) ) {
        throw new IOException( "ZIP did not create the expected plugin directory: " + finalPluginDir );
      }
      System.out.println( "[INFO] Plugin ready at " + finalPluginDir );
    } catch ( Exception e ) {
      System.out.println( "[ERROR] Big Data plugin download failed: " + e.getMessage() );
      e.printStackTrace( System.out );
      throw new RuntimeException( e );
    }
  }

  private static String resolveUrl( String pdiVersion, String variant ) {
    String override = System.getProperty( PROP_URL_OVERRIDE );
    if ( override != null && !override.isBlank() ) {
      return override.trim();
    }
    String base = DEFAULT_BASE_URLS.get( pdiVersion );
    if ( base == null ) {
      return null;
    }
    if ( !base.endsWith( "/" ) ) {
      base = base + "/";
    }
    return base + "pentaho-big-data-ee-plugin-" + variant + ".zip";
  }

  private static void downloadIfMissing( URI uri, Path dest ) throws IOException {
    if ( isValidZip( dest ) ) {
      System.out.println( "[INFO] ZIP already present, skipping download" );
      return;
    }
    Files.deleteIfExists( dest );
    Files.createDirectories( Objects.requireNonNull( dest.getParent() ) );
    Path tmp = dest.resolveSibling( dest.getFileName() + ".part" );
    System.out.println( "[INFO] Downloading " + uri + " ..." );
    try {
      HttpClient client = HttpClient.newBuilder().followRedirects( HttpClient.Redirect.NORMAL )
        .connectTimeout( Duration.ofSeconds( 30 ) ).build();
      HttpResponse<Path> response = client.send( authenticatedRequest( uri ).build(),
        HttpResponse.BodyHandlers.ofFile( tmp ) );
      ensureSuccess( response.statusCode(), uri );
      if ( !Files.exists( tmp ) || Files.size( tmp ) == 0 ) {
        throw new IOException( "Downloaded file is empty: " + uri );
      }
      try {
        Files.move( tmp, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING );
      } catch ( IOException e ) {
        Files.move( tmp, dest, StandardCopyOption.REPLACE_EXISTING );
      }
    } catch ( InterruptedException e ) {
      Thread.currentThread().interrupt();
      throw new IOException( "HTTP download interrupted", e );
    } catch ( IOException e ) {
      Files.deleteIfExists( tmp );
      throw e;
    }
    System.out.println( "[INFO] Download complete: " + dest + " (" + Files.size( dest ) + " bytes)" );
  }

  private static HttpRequest.Builder authenticatedRequest( URI uri ) {
    HttpRequest.Builder builder = HttpRequest.newBuilder( uri ).timeout( Duration.ofMinutes( 10 ) ).GET();
    if ( !ARTIFACTORY_HOST.equalsIgnoreCase( uri.getHost() ) ) {
      return builder;
    }
    String username = System.getenv( "MAVEN_USER" );
    String password = System.getenv( "MAVEN_PASSWORD" );
    if ( username == null || username.isBlank() || password == null || password.isBlank() ) {
      System.out.println( "[INFO] Maven credentials are not available in MAVEN_USER/MAVEN_PASSWORD" );
    } else {
      String creds = username + ":" + password;
      builder.header( "Authorization",
        "Basic " + Base64.getEncoder().encodeToString( creds.getBytes( StandardCharsets.UTF_8 ) ) );
    }
    return builder;
  }

  private static void ensureSuccess( int statusCode, URI uri ) throws IOException {
    if ( statusCode < 200 || statusCode >= 300 ) {
      String reason = statusCode == 401 || statusCode == 403
        ? ". Check MAVEN_USER and MAVEN_PASSWORD credentials."
        : statusCode == 404 ? ". Check the version, variant, and artifact path." : ".";
      throw new IOException( "HTTP " + statusCode + " while downloading " + uri + reason );
    }
  }

  private static boolean isValidZip( Path zipFile ) {
    if ( !Files.isRegularFile( zipFile ) ) {
      return false;
    }
    try ( ZipFile zip = new ZipFile( zipFile.toFile() ) ) {
      return zip.size() > 0;
    } catch ( IOException e ) {
      return false;
    }
  }

  private static void unzip( Path zipFile, Path unpackDir ) throws IOException {
    Files.createDirectories( unpackDir );
    System.out.println( "[INFO] Unzipping " + zipFile + " -> " + unpackDir );
    try ( ZipInputStream zis = new ZipInputStream( new BufferedInputStream( Files.newInputStream( zipFile ) ) ) ) {
      ZipEntry entry;
      while ( ( entry = zis.getNextEntry() ) != null ) {
        Path outPath = safeResolve( unpackDir, entry.getName() );
        if ( entry.isDirectory() ) {
          Files.createDirectories( outPath );
          continue;
        }
        Files.createDirectories( outPath.getParent() );
        try ( OutputStream out = new BufferedOutputStream( Files.newOutputStream( outPath ) ) ) {
          zis.transferTo( out );
        }
      }
    }
    System.out.println( "[INFO] Unzip complete" );
  }

  private static void cleanDirectoryContents( Path dir ) throws IOException {
    Files.createDirectories( dir );
    try ( Stream<Path> paths = Files.walk( dir ) ) {
      paths.sorted( Comparator.reverseOrder() )
        .filter( p -> !p.equals( dir ) )
        .forEach( p -> {
          try {
            Files.deleteIfExists( p );
          } catch ( IOException ignored ) {
            // best-effort
          }
        } );
    }
  }

  /** Prevents Zip Slip by ensuring the resolved path stays within the target directory. */
  private static Path safeResolve( Path targetDir, String entryName ) throws IOException {
    Path resolved = targetDir.resolve( entryName ).normalize();
    if ( !resolved.startsWith( targetDir ) ) {
      throw new IOException( "Blocked Zip Slip entry: " + entryName );
    }
    return resolved;
  }

  private static String required( String key ) {
    String value = System.getProperty( key );
    if ( value == null || value.isBlank() ) {
      throw new IllegalStateException( "Missing required system property: " + key );
    }
    return value.trim();
  }
}
