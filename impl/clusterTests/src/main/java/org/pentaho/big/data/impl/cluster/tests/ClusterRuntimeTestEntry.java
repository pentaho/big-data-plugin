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


package org.pentaho.big.data.impl.cluster.tests;

import org.pentaho.runtime.test.action.RuntimeTestAction;
import org.pentaho.runtime.test.action.impl.HelpUrlPayload;
import org.pentaho.runtime.test.action.impl.RuntimeTestActionImpl;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;
import org.pentaho.di.core.Const;

/**
 * This is a convenience class that will add a shim troubleshooting guide action if none is specified and the severity
 * is >= WARNING
 */
public class ClusterRuntimeTestEntry extends RuntimeTestResultEntryImpl {
  public static final String RUNTIME_TEST_RESULT_ENTRY_WITH_DEFAULT_SHIM_HELP_TROUBLESHOOTING_GUIDE =
    "RuntimeTestResultEntryWithDefaultShimHelp.TroubleshootingGuide";
  public static final String RUNTIME_TEST_RESULT_ENTRY_WITH_DEFAULT_SHIM_HELP_SHELL_DOC =
    "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc";
  public static final String RUNTIME_TEST_RESULT_ENTRY_WITH_DEFAULT_SHIM_HELP_SHELL_DOC_TITLE =
    "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Title";
  public static final String RUNTIME_TEST_RESULT_ENTRY_WITH_DEFAULT_SHIM_HELP_SHELL_DOC_HEADER =
    "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Header";
  private static final Class<?> PKG = ClusterRuntimeTestEntry.class;

  public ClusterRuntimeTestEntry( MessageGetterFactory messageGetterFactory, RuntimeTestEntrySeverity severity,
                                  String description, String message, DocAnchor docAnchor ) {
    this( messageGetterFactory, severity, description, message, null, docAnchor );
  }

  public ClusterRuntimeTestEntry( RuntimeTestEntrySeverity severity, String description, String message,
                                  RuntimeTestAction runtimeTestAction ) {
    this( severity, description, message, null, runtimeTestAction );
  }

  public ClusterRuntimeTestEntry( MessageGetterFactory messageGetterFactory,
                                  RuntimeTestResultEntry runtimeTestResultEntry, DocAnchor docAnchor ) {
    this( runtimeTestResultEntry.getSeverity(), runtimeTestResultEntry.getDescription(),
      runtimeTestResultEntry.getMessage(), runtimeTestResultEntry.getException(),
      getDefaultAction( messageGetterFactory, runtimeTestResultEntry, docAnchor ) );
  }

  public ClusterRuntimeTestEntry( MessageGetterFactory messageGetterFactory, RuntimeTestEntrySeverity severity,
                                  String description, String message, Throwable exception, DocAnchor docAnchor ) {
    this( severity, description, message, exception, createDefaultAction( messageGetterFactory, severity, docAnchor ) );
  }

  public ClusterRuntimeTestEntry( RuntimeTestEntrySeverity severity, String description, String message,
                                  Throwable exception, RuntimeTestAction runtimeTestAction ) {
    super( severity, description, message, exception, runtimeTestAction );
  }

  private static RuntimeTestAction getDefaultAction( MessageGetterFactory messageGetterFactory,
                                                     RuntimeTestResultEntry runtimeTestResultEntry,
                                                     DocAnchor docAnchor ) {
    RuntimeTestAction action = runtimeTestResultEntry.getAction();
    if ( action != null ) {
      return action;
    }
    return createDefaultAction( messageGetterFactory, runtimeTestResultEntry.getSeverity(), docAnchor );
  }

  private static RuntimeTestAction createDefaultAction( MessageGetterFactory messageGetterFactory,
                                                        RuntimeTestEntrySeverity severity, DocAnchor docAnchor ) {
    if ( severity == null || severity.ordinal() >= RuntimeTestEntrySeverity.WARNING.ordinal() ) {
      MessageGetter messageGetter = messageGetterFactory.create( PKG );
      String docUrl =
          Const.getDocUrl( messageGetter.getMessage( RUNTIME_TEST_RESULT_ENTRY_WITH_DEFAULT_SHIM_HELP_SHELL_DOC ) );
      if ( docAnchor != null ) {
        docUrl += messageGetter.getMessage( docAnchor.getAnchorTextKey() );
      }
      return new RuntimeTestActionImpl( messageGetter.getMessage(
        RUNTIME_TEST_RESULT_ENTRY_WITH_DEFAULT_SHIM_HELP_TROUBLESHOOTING_GUIDE ),
        docUrl, severity, new HelpUrlPayload( messageGetterFactory,
          messageGetter.getMessage(
            RUNTIME_TEST_RESULT_ENTRY_WITH_DEFAULT_SHIM_HELP_SHELL_DOC_TITLE ),
          messageGetter.getMessage(
            RUNTIME_TEST_RESULT_ENTRY_WITH_DEFAULT_SHIM_HELP_SHELL_DOC_HEADER ),
          docUrl ) );
    }
    return null;
  }

  public enum DocAnchor {
    GENERAL( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Anchor.General" ),
    SHIM_LOAD( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Anchor.ShimLoad" ),
    CLUSTER_CONNECT( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Anchor.ClusterConnect" ),
    CLUSTER_CONNECT_GATEWAY( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Anchor.ClusterConnectGateway" ),
    ACCESS_DIRECTORY( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Anchor.AccessDirectory" ),
    OOZIE( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Anchor.Oozie" ),
    ZOOKEEPER( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Anchor.Zookeeper" ),
    KAFKA( "RuntimeTestResultEntryWithDefaultShimHelp.Shell.Doc.Anchor.Kafka" );
    private final String anchorTextKey;

    DocAnchor( String anchorTextKey ) {
      this.anchorTextKey = anchorTextKey;
    }

    public String getAnchorTextKey() {
      return anchorTextKey;
    }
  }
}
