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


package org.pentaho.di.job.entries.spark;

import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.Kernel32Util;
import com.sun.jna.platform.win32.Tlhelp32;
import com.sun.jna.platform.win32.WinDef;
import com.sun.jna.platform.win32.WinNT;
import com.sun.jna.win32.W32APIOptions;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class WinProcess {

  private int pid;
  private WinNT.HANDLE handle;

  private static final int PROCESS_QUERY_INFORMATION = 0x0400;
  private static final int PROCESS_SUSPEND_RESUME = 0x0800;
  private static final int PROCESS_TERMINATE = 0x0001;
  private static final int PROCESS_SYNCHRONIZE = 0x00100000;

  WinProcess( int pid ) throws IOException {
    handle = Kernel32.INSTANCE
      .OpenProcess( PROCESS_QUERY_INFORMATION | PROCESS_SUSPEND_RESUME | PROCESS_TERMINATE | PROCESS_SYNCHRONIZE, false,
        pid );
    if ( handle == null ) {
      throw new IOException(
        "OpenProcess failed: " + Kernel32Util.formatMessageFromLastErrorCode( Kernel32.INSTANCE.GetLastError() ) );
    }
    this.pid = pid;
  }

  public void terminate() {
    Kernel32.INSTANCE.TerminateProcess( handle, 0 );
  }

  private List<WinProcess> getChildProcesses() throws IOException {

    int childPID;
    List<WinProcess> processList = new ArrayList<>();
    List<Integer> pidList = new ArrayList<>();
    pidList.add( pid );
    int parentPID;

    Kernel32 kernel32 = Native.loadLibrary( Kernel32.class, W32APIOptions.UNICODE_OPTIONS );
    Tlhelp32.PROCESSENTRY32.ByReference processEntry = new Tlhelp32.PROCESSENTRY32.ByReference();
    WinNT.HANDLE snapshot = kernel32.CreateToolhelp32Snapshot( Tlhelp32.TH32CS_SNAPPROCESS, new WinDef.DWORD( 0 ) );
    try {
      while ( kernel32.Process32Next( snapshot, processEntry ) ) {
        parentPID = processEntry.th32ParentProcessID.intValue();
        if ( pidList.contains( parentPID ) ) {
          childPID = processEntry.th32ProcessID.intValue();
          pidList.add( childPID );
          processList.add( new WinProcess( childPID ) );
        }
      }
    } finally {
      kernel32.CloseHandle( snapshot );
    }
    return processList;
  }

  public String killChildProcesses() throws IOException {
    StringBuilder builder = new StringBuilder();
    if ( Platform.isWindows() ) {
      List<WinProcess> children = getChildProcesses();
      if ( !children.isEmpty() ) {
        for ( WinProcess child : children ) {
          builder.append( child.getWinProcessPID() + " " );
          child.terminate();
        }
      }
    }
    return builder.toString().trim();
  }

  public static int getPID( Process proc ) {
    int pid = -1;
    try {
      if ( proc.getClass().getName().equals( "java.lang.Win32Process" ) || proc.getClass().getName()
        .equals( "java.lang.ProcessImpl" ) ) {
        Field f = proc.getClass().getDeclaredField( "handle" );
        f.setAccessible( true );
        long handl = f.getLong( proc );

        Kernel32 kernel = Kernel32.INSTANCE;
        WinNT.HANDLE handle = new WinNT.HANDLE();
        handle.setPointer( Pointer.createConstant( handl ) );
        pid = kernel.GetProcessId( handle );
      }
    } catch ( Exception e ) {
      e.printStackTrace();
    }
    return pid;
  }

  public int getWinProcessPID() {
    return pid;
  }
}
