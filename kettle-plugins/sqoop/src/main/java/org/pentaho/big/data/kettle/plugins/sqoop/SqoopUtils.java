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


package org.pentaho.big.data.kettle.plugins.sqoop;

import org.apache.commons.lang.StringUtils;
import org.pentaho.big.data.kettle.plugins.job.JobEntryMode;
import org.pentaho.big.data.kettle.plugins.job.PropertyEntry;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;


/**
 * Collection of utility methods used to support integration with Apache Sqoop.
 */
public class SqoopUtils {
  /**
   * Prefix to append before an argument's name when building up a list of command-line arguments, e.g. "--"
   */
  public static final String ARG_PREFIX = "--";
  public static final String ARG_PREFIX_1 = "-";
  public static final String ARG_D = "-D";

  // Properties used to escape/unescape strings for command line string (de)serialization
  private static final String WHITESPACE = " ";
  private static final String EQUALS = "=";
  private static final String QUOTE = "\"";
  private static final Pattern WHITESPACE_PATTERN = Pattern.compile( " " );
  private static final Pattern QUOTE_PATTERN = Pattern.compile( "\"" );
  private static final Pattern BACKSLASH_PATTERN = Pattern.compile( "\\\\" );
  private static final Pattern EQUALS_PATTERN = Pattern.compile( "=" );
  // Simple map of Patterns that match an escape sequence and a replacement string to replace them with to escape them
  private static final Object[][] ESCAPE_SEQUENCES = new Object[][] {
    new Object[] { Pattern.compile( "\t" ), "\\\\t" }, new Object[] { Pattern.compile( "\b" ), "\\\\b" },
    new Object[] { Pattern.compile( "\n" ), "\\\\n" }, new Object[] { Pattern.compile( "\r" ), "\\\\r" },
    new Object[] { Pattern.compile( "\f" ), "\\\\f" } };

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

          if ( s.startsWith( ARG_D ) ) {
            handleCustomOption( args, s, tokenizer, variableSpace );
            continue;
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

    Map<String, String> argValues = new HashMap<>();
    // save the order
    Map<String, String> customArgValues = new LinkedHashMap<>();
    int i = 0;
    int peekAhead = i;
    while ( i < args.size() ) {
      String arg = args.get( i );
      int prefLen = isArgName( arg );
      if ( prefLen > 0 ) {
        arg = arg.substring( prefLen );
      }

      String value = null;
      peekAhead = i + 1;
      if ( peekAhead < args.size() ) {
        value = args.get( peekAhead );
      }

      if ( ARG_D.equals( arg ) ) {
        int index = value.indexOf( EQUALS );
        String customArg = value.substring( 0, index );
        String customValue = value.substring( index + 1 );

        if ( variableSpace != null ) {
          customArg = variableSpace.environmentSubstitute( value );
          customValue = variableSpace.environmentSubstitute( value );
        }

        customArgValues.put( customArg, customValue );
        i += 2;
        continue;
      }

      if ( isArgName( value ) > 0 ) {
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
    setCustomArgumentStringValues( config, customArgValues );
  }

  /**
   * Does the string reprsent an argument name as provided on the command line? Format: "--argname"
   *
   * @param s
   *          Possible argument name
   * @return {@code true} if the string represents an argument name (is prefixed with ARG_PREFIX)
   */
  private static int isArgName( String s ) {
    if ( s != null ) {
      if ( s.startsWith( ARG_PREFIX ) && s.length() > ARG_PREFIX.length() ) {
        return ARG_PREFIX.length();
      }
      if ( ARG_D.equals( s ) ) {
        return 0;
      }
      if ( s.startsWith( ARG_PREFIX_1 ) && s.length() > ARG_PREFIX_1.length() ) {
        return ARG_PREFIX_1.length();
      }
    }

    return 0;
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

  private static void setCustomArgumentStringValues( SqoopConfig config, Map<String, String> customArgValues ) {
    config.getCustomArguments().clear();

    for ( Iterator<Map.Entry<String, String>> iterator = customArgValues.entrySet().iterator(); iterator.hasNext(); ) {
      Map.Entry<String, String> entry = iterator.next();
      config.getCustomArguments().add( new PropertyEntry( entry.getKey(), entry.getValue() ) );
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
   *           when config mode is {@link JobEntryMode#ADVANCED_COMMAND_LINE} and the command line
   *           could not be parsed
   */
  public static List<String> getCommandLineArgs( SqoopConfig config, VariableSpace variableSpace ) throws IOException {
    List<String> args = new ArrayList<String>();

    if ( JobEntryMode.ADVANCED_COMMAND_LINE.equals( config.getModeAsEnum() ) ) {
      return parseCommandLine( config.getCommandLine(), variableSpace, true );
    } else {

      appendCustomArguments( args, config, variableSpace );
      appendArguments( args, SqoopUtils.findAllArguments( config ), variableSpace );

      return args;
    }
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
    List<String> customBuffer = new ArrayList<String>();

    // Add custom arguments as they must appear before tool specific arguments
    for ( PropertyEntry entry : config.getCustomArguments() ) {
      appendCustomArgument( customBuffer, entry, variableSpace, true );
    }

    for ( Iterator<String> iterator = customBuffer.iterator(); iterator.hasNext(); ) {
      sb.append( iterator.next() );
      if ( iterator.hasNext() ) {
        sb.append( WHITESPACE );
      }
    }

    for ( ArgumentWrapper arg : SqoopUtils.findAllArguments( config ) ) {
      List<String> buffer = new ArrayList<String>( 4 );
      appendArgument( buffer, arg, variableSpace );
      if ( !buffer.isEmpty() ) {
        buffers.add( buffer );
      }
    }

    if ( !customBuffer.isEmpty() && !buffers.isEmpty() ) {
      sb.append( WHITESPACE );
    }

    Iterator<List<String>> buffersIter = buffers.iterator();
    while ( buffersIter.hasNext() ) {
      List<String> buffer = buffersIter.next();
      sb.append( buffer.get( 0 ) );
      if ( buffer.size() == 2 ) {
        sb.append( WHITESPACE );
        // Escape value and add
        sb.append( quote( escapeBackslash( buffer.get( 1 ) ) ) );
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
    if ( !orig.equals( s ) || WHITESPACE_PATTERN.matcher( s ).find() || BACKSLASH_PATTERN.matcher( s ).find() || EQUALS_PATTERN.matcher( s ).find() ) {
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
    if ( arg.getName().equals( "password" ) ) {
      value = Encr.decryptPasswordOptionallyEncrypted( value );
    }
    if ( arg.isFlag() && Boolean.parseBoolean( value ) ) {
      args.add( arg.getPrefix() + arg.getName() );
    } else if ( !arg.isFlag() && value != null ) {
      if ( !StringUtil.isEmpty( value ) ) {
        args.add( arg.getPrefix() + arg.getName() );
        args.add( value );
      }
    }
  }

  private static void appendCustomArguments( List<String> args, SqoopConfig config, VariableSpace variableSpace ) {
    for ( PropertyEntry entry : config.getCustomArguments() ) {
      appendCustomArgument( args, entry, variableSpace, false );
    }
  }

  private static void appendCustomArgument( List<String> args, PropertyEntry arg, VariableSpace variableSpace, boolean quote ) {
    String key = arg.getKey();
    String value = arg.getValue();

    // ignore if both key and value are blank
    if ( StringUtils.isBlank( key ) && StringUtils.isBlank( value ) ) {
      return;
    }

    key = StringUtils.defaultIfBlank( arg.getKey(), "null" );
    value = StringUtils.defaultIfBlank( arg.getValue(), "null" );

    if ( variableSpace != null ) {
      key = variableSpace.environmentSubstitute( key );
      value = variableSpace.environmentSubstitute( value );
    }

    if ( quote ) {
      value = quote( escapeBackslash( value ) );
    }

    args.add( ARG_D );
    args.add( key + EQUALS + value );
  }

  private static String escapeBackslash( String s ) {
    return BACKSLASH_PATTERN.matcher( s ).replaceAll( "\\\\\\\\" );
  }

  private static void handleCustomOption( List<String> args, String option, StreamTokenizer tokenizer, VariableSpace variableSpace ) throws IOException {
    String key = null;
    String value = null;

    args.add( ARG_D );
    if ( ARG_D.equals( option ) ) {
      tokenizer.nextToken();
      key = tokenizer.sval;
    } else {
      key = option.substring( ARG_D.length() );
    }

    if ( key.contains( EQUALS ) ) {
      if ( key.endsWith( EQUALS ) ) {
        key = key.substring( 0, key.length() - 1 );
        tokenizer.nextToken();
        value = tokenizer.sval;
      } else {
        String[] split = key.split( EQUALS );
        key = split[0];
        value = split[1];
      }
    } else {
      tokenizer.nextToken();
      value = tokenizer.sval;
    }
    if ( variableSpace != null ) {
      key = variableSpace.environmentSubstitute( key );
      value = variableSpace.environmentSubstitute( value );
    }
    args.add( key + EQUALS + escapeEscapeSequences( value ) );
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
    Set<ArgumentWrapper> arguments = new TreeSet<ArgumentWrapper>(
        new Comparator<ArgumentWrapper>() {
          @Override
          /*
           * Sort by order then by name
           */
          public int compare( ArgumentWrapper o1, ArgumentWrapper o2 ) {
            int diff = o1.getOrder() - o2.getOrder();
            if ( diff != 0 ) {
              return diff;
            }

            return o1.getName().compareTo( o2.getName() );
          }
        }
    );

    Class<?> aClass = o.getClass();
    while ( aClass != null ) {
      for ( Field f : aClass.getDeclaredFields() ) {
        if ( f.isAnnotationPresent( CommandLineArgument.class ) ) {
          CommandLineArgument anno = f.getAnnotation( CommandLineArgument.class );
          String fieldName = f.getName().substring( 0, 1 ).toUpperCase() + f.getName().substring( 1 );
          Method getter = findMethod( aClass, fieldName, null, "get", "is" );
          Method setter = findMethod( aClass, fieldName, new Class<?>[] { f.getType() }, "set" );
          arguments.add( new ArgumentWrapper( anno.name(), getDisplayName( anno ), anno.flag(),
              anno.prefix(), anno.order(), o, getter, setter ) );
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
   * @return {@link CommandLineArgument#displayName()} or, if not set,
   *         {@link CommandLineArgument#name()}
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
