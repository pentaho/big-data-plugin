/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.job.entries.sqoop;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.ArgumentWrapper;
import org.pentaho.di.job.CommandLineArgument;
import org.pentaho.di.job.JobEntryMode;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

/**
 * Collection of utility methods used to support integration with Apache Sqoop.
 */
public class SqoopUtils {
  /**
   * Prefix to append before an argument's name when building up a list of command-line arguments, e.g. "--"
   */
  public static final String ARG_PREFIX = "--";

  // Properties used to escape/unescape strings for command line string (de)serialization
  private static final String WHITESPACE = " ";
  private static final String QUOTE = "\"";
  private static final Pattern WHITESPACE_PATTERN = Pattern.compile( " " );
  private static final Pattern QUOTE_PATTERN = Pattern.compile( "\"" );
  private static final Pattern BACKSLASH_PATTERN = Pattern.compile( "\\\\" );
  // Simple map of Patterns that match an escape sequence and a replacement string to replace them with to escape them
  private static final Object[][] ESCAPE_SEQUENCES = new Object[][] {
    new Object[] { Pattern.compile( "\t" ), "\\\\t" }, new Object[] { Pattern.compile( "\b" ), "\\\\b" },
    new Object[] { Pattern.compile( "\n" ), "\\\\n" }, new Object[] { Pattern.compile( "\r" ), "\\\\r" },
    new Object[] { Pattern.compile( "\f" ), "\\\\f" } };

  /**
   * Configure a {@link SqoopConfig}'s Namenode and Jobtracker connection information based off a Hadoop Configuration's
   * settings. These properties are parsed from {@code fs.default.name} and {@code mapred.job.tracker} properties.
   * 
   * @param config
   *          Sqoop configuration to update
   * @param c
   *          Hadoop configuration to parse connection information from
   */
  public static void configureConnectionInformation( SqoopConfig config, HadoopShim shim, Configuration c ) {
    String[] namenodeInfo = shim.getNamenodeConnectionInfo( c );
    if ( namenodeInfo != null ) {
      if ( namenodeInfo[0] != null ) {
        config.setNamenodeHost( namenodeInfo[0] );
      }
      if ( !"-1".equals( namenodeInfo[1] ) ) {
        config.setNamenodePort( namenodeInfo[1] );
      }
    }
    String[] jobtrackerInfo = shim.getJobtrackerConnectionInfo( c );
    if ( jobtrackerInfo != null ) {
      if ( jobtrackerInfo[0] != null ) {
        config.setJobtrackerHost( jobtrackerInfo[0] );
      }
      if ( jobtrackerInfo[1] != null ) {
        config.setJobtrackerPort( jobtrackerInfo[1] );
      }
    }
  }

  /**
   * Parse a string into arguments as if it were provided on the command line.
   * 
   * @param commandLineString
   *          A command line string, e.g. "sqoop import --table test --connect jdbc:mysql://bogus/bogus"
   * @param variableSpace
   *          Context for resolving variable names. If {@code null}, no variable resolution we happen.
   * @param ignoreSqoopCommand
   *          If set, the first "sqoop <tool>" arguments will be ignored, e.g. "sqoop import" or "sqoop export".
   * @return List of parsed arguments
   * @throws IOException
   *           when the command line could not be parsed
   */
  public static List<String> parseCommandLine( String commandLineString, VariableSpace variableSpace,
      boolean ignoreSqoopCommand ) throws IOException {
    List<String> args = new ArrayList<String>();
    StringReader reader = new StringReader( commandLineString );
    try {
      StreamTokenizer tokenizer = new StreamTokenizer( reader );
      // Treat a dash as an ordinary character so it gets included in the token
      tokenizer.ordinaryChar( '-' );
      tokenizer.ordinaryChar( '.' );
      tokenizer.ordinaryChars( '0', '9' );
      // Treat all characters as word characters so nothing is parsed out
      tokenizer.wordChars( '\u0000', '\uFFFF' );

      // Re-add whitespace characters
      tokenizer.whitespaceChars( 0, ' ' );

      // Use " and ' as quote characters
      tokenizer.quoteChar( '"' );
      tokenizer.quoteChar( '\'' );

      // Flag to indicate if the next token needs to be skipped (used to control skipping of the first two arguments,
      // e.g. "sqoop <tool>")
      boolean skipToken = false;
      // Add all non-null string values tokenized from the string to the argument list
      while ( tokenizer.nextToken() != StreamTokenizer.TT_EOF ) {
        if ( tokenizer.sval != null ) {
          String s = tokenizer.sval;
          if ( variableSpace != null ) {
            s = variableSpace.environmentSubstitute( s );
          }
          if ( ignoreSqoopCommand && args.isEmpty() ) {
            // If we encounter "sqoop <name>" we should skip the first two arguments so we can support copy/paste of
            // arguments directly
            // from a working command line
            if ( "sqoop".equals( s ) ) {
              skipToken = true;
              continue; // skip this one and the next
            } else if ( skipToken ) {
              ignoreSqoopCommand = false; // Don't attempt to ignore any more commands
              // Skip this token too, reset the flag so we no longer skip any tokens, and continue parsing
              skipToken = false;
              continue;
            }
          }
          args.add( escapeEscapeSequences( s ) );
        }
      }
    } finally {
      reader.close();
    }
    return args;
  }

  /**
   * Configure a {@link SqoopConfig} object from a command line string. Variables will be replaced if
   * {@code variableSpace} is provided.
   * 
   * @param config
   *          Configuration to update
   * @param commandLineString
   *          Command line string to parse and update config with (string will be parsed via
   *          {@link #parseCommandLine(String, org.pentaho.di.core.variables.VariableSpace, boolean)})
   * @param variableSpace
   *          Context for variable substitution
   * @throws IOException
   *           error parsing command line string
   * @throws KettleException
   *           Error setting properties from parsed command line arguments
   */
  public static void configureFromCommandLine(
      SqoopConfig config, String commandLineString, VariableSpace variableSpace ) throws IOException, KettleException {
    List<String> args = parseCommandLine( commandLineString, variableSpace, true );

    Map<String, String> argValues = new HashMap<String, String>();
    int i = 0;
    int peekAhead = i;
    while ( i < args.size() ) {
      String arg = args.get( i );
      if ( isArgName( arg ) ) {
        arg = arg.substring( ARG_PREFIX.length() );
      }

      String value = null;
      peekAhead = i + 1;
      if ( peekAhead < args.size() ) {
        value = args.get( peekAhead );
      }

      if ( isArgName( value ) ) {
        // Current arg is possibly a boolean flag, set value to null now
        value = null;
        // We're only consuming one element
        i += 1;
      } else {
        // value is a real value, make sure to substitute variables if we can
        if ( variableSpace != null ) {
          value = variableSpace.environmentSubstitute( value );
        }
        i += 2;
      }

      argValues.put( arg, value );
    }

    setArgumentStringValues( config, argValues );
  }

  /**
   * Does the string reprsent an argument name as provided on the command line? Format: "--argname"
   * 
   * @param s
   *          Possible argument name
   * @return {@code true} if the string represents an argument name (is prefixed with ARG_PREFIX)
   */
  private static boolean isArgName( String s ) {
    return s != null && s.startsWith( ARG_PREFIX ) && s.length() > ARG_PREFIX.length();
  }

  /**
   * Updates arguments of {@code config} based on the map of argument values. All other arguments will be cleared from
   * {@code config}.
   * 
   * @param config
   *          Configuration object to update
   * @param args
   *          Argument name and value pairs
   * @throws KettleException
   *           when we cannot set the value of the argument either because it doesn't exist or any other reason
   */
  protected static void setArgumentStringValues( SqoopConfig config, Map<String, String> args ) throws KettleException {
    Class<?> aClass = config.getClass();

    while ( aClass != null ) {
      for ( Field field : aClass.getDeclaredFields() ) {
        if ( field.isAnnotationPresent( CommandLineArgument.class ) ) {
          CommandLineArgument arg = field.getAnnotation( CommandLineArgument.class );

          String value = pickupArgumentValueFor( arg, args );

          try {
            String fieldName = field.getName().substring( 0, 1 ).toUpperCase() + field.getName().substring( 1 );
            Method setter = findMethod( config.getClass(), fieldName, new Class[] { String.class }, "set" );
            setter.invoke( config, value );
          } catch ( Exception ex ) {
            throw new KettleException( "Cannot set value of argument \"" + arg.name() + "\" to \"" + value + "\"", ex );
          }
        }
      }
      aClass = aClass.getSuperclass();
    }

    // If any arguments weren't handled report them as errors
    if ( !args.isEmpty() ) {
      StringBuilder sb = new StringBuilder();
      Iterator<String> i = args.keySet().iterator();
      while ( i.hasNext() ) {
        sb.append( i.next() );
        if ( i.hasNext() ) {
          sb.append( ", " );
        }
      }
      throw new KettleException( BaseMessages.getString( AbstractSqoopJobEntry.class, "ErrorUnknownArguments", sb ) );
    }
  }

  private static String pickupArgumentValueFor( CommandLineArgument arg, Map<String, String> args )
    throws KettleException {
    String argumentName = arg.name();
    if ( args.containsKey( argumentName ) ) {

      // Remove the value from the map to indicate it has been processed
      String value = args.remove( argumentName );

      if ( arg.flag() ) {
        return Boolean.TRUE.toString();
      }

      if ( StringUtil.isEmpty( value ) ) {
        throw new KettleException( BaseMessages.getString( AbstractSqoopJobEntry.class, "ErrorProhibitedEmptyString",
            argumentName ) );
      }

      return value;
    }

    return null;
  }

  /**
   * Generate a list of command line arguments and their values for arguments that require them.
   * 
   * @param config
   *          Sqoop configuration to build a list of command line arguments from
   * @param variableSpace
   *          Variable space to look up argument values from. May be {@code null}
   * @return All the command line arguments for this configuration object
   * @throws IOException
   *           when config mode is {@link org.pentaho.di.job.JobEntryMode#ADVANCED_COMMAND_LINE} and the command line
   *           could not be parsed
   */
  public static List<String> getCommandLineArgs( SqoopConfig config, VariableSpace variableSpace ) throws IOException {
    List<String> args = new ArrayList<String>();

    if ( JobEntryMode.ADVANCED_COMMAND_LINE.equals( config.getModeAsEnum() ) ) {
      return parseCommandLine( config.getCommandLine(), variableSpace, true );
    }

    appendArguments( args, SqoopUtils.findAllArguments( config ), variableSpace );

    return args;
  }

  /**
   * Generate a command line string for the given configuration. Replace variables with the values from
   * {@code variableSpace} if provided.
   * 
   * @param config
   *          Sqoop configuration
   * @param variableSpace
   *          Context for variable substitutions
   * @return String-representation of the current configuration values. Variable tokens will be replaced if
   *         {@code variableSpace} is provided.
   */
  public static String generateCommandLineString( SqoopConfig config, VariableSpace variableSpace ) {
    StringBuilder sb = new StringBuilder();
    List<List<String>> buffers = new ArrayList<List<String>>();

    for ( ArgumentWrapper arg : SqoopUtils.findAllArguments( config ) ) {
      List<String> buffer = new ArrayList<String>( 4 );
      appendArgument( buffer, arg, variableSpace );
      if ( !buffer.isEmpty() ) {
        buffers.add( buffer );
      }
    }

    Iterator<List<String>> buffersIter = buffers.iterator();
    while ( buffersIter.hasNext() ) {
      List<String> buffer = buffersIter.next();
      sb.append( buffer.get( 0 ) );
      if ( buffer.size() == 2 ) {
        sb.append( WHITESPACE );
        // Escape value and add
        sb.append( quote( buffer.get( 1 ) ) );
      }
      if ( buffersIter.hasNext() ) {
        sb.append( WHITESPACE );
      }
    }

    return sb.toString();
  }

  /**
   * Escapes known Java escape sequences. See {@link #ESCAPE_SEQUENCES} for the list of escape sequences we escape here.
   * 
   * @param s
   *          String to escape
   * @return Escaped string where all escape sequences are properly escaped
   */
  protected static String escapeEscapeSequences( String s ) {
    for ( Object[] escapeSequence : ESCAPE_SEQUENCES ) {
      s = ( (Pattern) escapeSequence[0] ).matcher( s ).replaceAll( (String) escapeSequence[1] );
    }
    return s;
  }

  /**
   * If any whitespace is detected the string will be quoted. If any quotes exist in the string they will be escaped.
   * 
   * @param s
   *          String to quote
   * @return A quoted version of {@code s} if whitespace exists in the string, otherwise unmodified {@code s}.
   */
  protected static String quote( String s ) {
    final String orig = s;
    s = QUOTE_PATTERN.matcher( s ).replaceAll( "\\\\\"" );
    // Make sure the string is quoted if it contains a quote character, whitespace or has a backslash
    if ( !orig.equals( s ) || WHITESPACE_PATTERN.matcher( s ).find() || BACKSLASH_PATTERN.matcher( s ).find() ) {
      s = QUOTE + s + QUOTE;
    }
    return s;
  }

  /**
   * Add all {@link ArgumentWrapper}s to a list of arguments
   * 
   * @param args
   *          Arguments to append to
   * @param arguments
   *          Arguments to append
   * @param variableSpace
   *          Variable space to look up argument values from. May be {@code null}.
   */
  protected static void appendArguments( List<String> args, Set<? extends ArgumentWrapper> arguments,
      VariableSpace variableSpace ) {
    for ( ArgumentWrapper ai : arguments ) {
      appendArgument( args, ai, variableSpace );
    }
  }

  /**
   * Append this argument to a list of arguments if it has a value or if it's a flag.
   * 
   * @param args
   *          List of arguments to append to
   */
  protected static void appendArgument( List<String> args, ArgumentWrapper arg, VariableSpace variableSpace ) {
    String value = arg.getValue();
    if ( variableSpace != null ) {
      value = variableSpace.environmentSubstitute( value );
    }
    if ( arg.isFlag() && Boolean.parseBoolean( value ) ) {
      args.add( ARG_PREFIX + arg.getName() );
    } else if ( !arg.isFlag() && value != null ) {
      if ( !StringUtil.isEmpty( value ) ) {
        args.add( ARG_PREFIX + arg.getName() );
        args.add( value );
      }
    }
  }

  /**
   * Find all fields annotated with {@link CommandLineArgument} in the class provided. All arguments must have valid
   * JavaBeans-style getter and setter methods in the object.
   * 
   * @param o
   *          Object to look for arguments in
   * @return Ordered set of arguments representing all {@link CommandLineArgument}-annotated fields in {@code o}
   */
  public static Set<? extends ArgumentWrapper> findAllArguments( Object o ) {
    Set<ArgumentWrapper> arguments = new LinkedHashSet<ArgumentWrapper>();

    Class<?> aClass = o.getClass();
    while ( aClass != null ) {
      for ( Field f : aClass.getDeclaredFields() ) {
        if ( f.isAnnotationPresent( CommandLineArgument.class ) ) {
          CommandLineArgument anno = f.getAnnotation( CommandLineArgument.class );
          String fieldName = f.getName().substring( 0, 1 ).toUpperCase() + f.getName().substring( 1 );
          Method getter = findMethod( aClass, fieldName, null, "get", "is" );
          Method setter = findMethod( aClass, fieldName, new Class<?>[] { f.getType() }, "set" );
          arguments.add( new ArgumentWrapper( anno.name(), getDisplayName( anno ), anno.flag(), o, getter, setter ) );
        }
      }
      aClass = aClass.getSuperclass();
    }

    return arguments;
  }

  /**
   * Determine the display name for the command line argument.
   * 
   * @param anno
   *          Command line argument
   * @return {@link org.pentaho.di.job.CommandLineArgument#displayName()} or, if not set,
   *         {@link org.pentaho.di.job.CommandLineArgument#name()}
   */
  public static String getDisplayName( CommandLineArgument anno ) {
    return StringUtil.isEmpty( anno.displayName() ) ? anno.name() : anno.displayName();
  }

  /**
   * Finds a method in the given class or any super class with the name {@code prefix + methodName} that accepts 0
   * parameters.
   * 
   * @param aClass
   *          Class to search for method in
   * @param methodName
   *          Camelcase'd method name to search for with any of the provided prefixes
   * @param parameterTypes
   *          The parameter types the method signature must match.
   * @param prefixes
   *          Prefixes to prepend to {@code methodName} when searching for method names, e.g. "get", "is"
   * @return The first method found to match the format {@code prefix + methodName}
   */
  public static Method findMethod( Class<?> aClass, String methodName, Class<?>[] parameterTypes, String... prefixes ) {
    for ( String prefix : prefixes ) {
      try {
        return aClass.getDeclaredMethod( prefix + methodName, parameterTypes );
      } catch ( NoSuchMethodException ex ) {
        // ignore, continue searching prefixes
      }
    }
    // If no method found with any prefixes search the super class
    aClass = aClass.getSuperclass();
    return aClass == null ? null : findMethod( aClass, methodName, parameterTypes, prefixes );
  }

}
