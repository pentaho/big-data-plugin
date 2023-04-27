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


public class ChildProcessTester implements Runnable {

    private String pattern = "child process started";

    @Override public void run() {
        for ( int i = 0; i < 1000; i++ ) {
            System.out.println( Thread.currentThread().getName() + " " + i );
            System.out.println( pattern );
            try {
                Thread.sleep( 10000 );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
        }
    }

    public static void main( String[] args ) {
        Thread thread = new Thread( new ChildProcessTester() );
        thread.start();
    }
}
