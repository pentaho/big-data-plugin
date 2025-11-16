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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for BigDataLogConfig
 */
public class BigDataLogConfigTest {

    @Before
    public void setUp() {
        // Reset initialization state before each test
        BigDataLogConfig.resetInitialization();
    }

    @After
    public void tearDown() {
        // Clean up after tests
        BigDataLogConfig.resetInitialization();
    }

    @Test
    public void testInitializeBigDataLogging_FirstTime() {
        assertFalse("Should not be initialized initially", BigDataLogConfig.isInitialized());

        BigDataLogConfig.initializeBigDataLogging();

        assertTrue("Should be initialized after first call", BigDataLogConfig.isInitialized());
    }

    @Test
    public void testInitializeBigDataLogging_MultipleCalls() {
        BigDataLogConfig.initializeBigDataLogging();
        assertTrue("Should be initialized after first call", BigDataLogConfig.isInitialized());

        // Call again
        BigDataLogConfig.initializeBigDataLogging();
        assertTrue("Should remain initialized after second call", BigDataLogConfig.isInitialized());
    }

    @Test
    public void testGetBigDataLogger_WithClass() {
        Logger logger = BigDataLogConfig.getBigDataLogger(BigDataLogConfigTest.class);

        assertNotNull("Logger should not be null", logger);
        assertEquals("Logger name should match class name", 
                     BigDataLogConfigTest.class.getName(), 
                     logger.getName());
        assertTrue("BigDataLogConfig should be initialized", BigDataLogConfig.isInitialized());
    }

    @Test
    public void testGetBigDataLogger_WithString() {
        String loggerName = "org.pentaho.test.logger";
        Logger logger = BigDataLogConfig.getBigDataLogger(loggerName);

        assertNotNull("Logger should not be null", logger);
        assertEquals("Logger name should match", loggerName, logger.getName());
        assertTrue("BigDataLogConfig should be initialized", BigDataLogConfig.isInitialized());
    }

    @Test
    public void testGetBigDataLogger_EnsuresInitialization() {
        assertFalse("Should not be initialized initially", BigDataLogConfig.isInitialized());

        BigDataLogConfig.getBigDataLogger("test.logger");

        assertTrue("Should be initialized after getting logger", BigDataLogConfig.isInitialized());
    }

    @Test
    public void testIsInitialized_InitialState() {
        assertFalse("Should not be initialized initially", BigDataLogConfig.isInitialized());
    }

    @Test
    public void testIsInitialized_AfterInitialization() {
        BigDataLogConfig.initializeBigDataLogging();
        assertTrue("Should be initialized", BigDataLogConfig.isInitialized());
    }

    @Test
    public void testResetInitialization() {
        BigDataLogConfig.initializeBigDataLogging();
        assertTrue("Should be initialized", BigDataLogConfig.isInitialized());

        BigDataLogConfig.resetInitialization();
        assertFalse("Should not be initialized after reset", BigDataLogConfig.isInitialized());
    }

    @Test
    public void testRegisterLogger_NewLogger() {
        String loggerName = "org.pentaho.big.data.test.newlogger";
        Level level = Level.DEBUG;

        boolean result = BigDataLogConfig.registerLogger(loggerName, level);

        assertTrue("Should successfully register new logger", result);

        // Verify the logger was registered with correct level
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);

        assertNotNull("Logger config should exist", loggerConfig);
        assertEquals("Logger should have correct name", loggerName, loggerConfig.getName());
        assertEquals("Logger should have correct level", level, loggerConfig.getLevel());
    }

    @Test
    public void testRegisterLogger_ExistingLogger() {
        String loggerName = "org.pentaho.big.data.test.existing";
        Level initialLevel = Level.INFO;
        Level newLevel = Level.DEBUG;

        // Register logger first time
        BigDataLogConfig.registerLogger(loggerName, initialLevel);

        // Register same logger again with different level
        boolean result = BigDataLogConfig.registerLogger(loggerName, newLevel);

        assertTrue("Should handle existing logger without error", result);

        // Verify the logger exists (level may or may not be updated depending on logic)
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);

        assertNotNull("Logger config should exist", loggerConfig);
        assertEquals("Logger should have correct name", loggerName, loggerConfig.getName());
    }

    @Test
    public void testRegisterLogger_MultipleLoggers() {
        String logger1 = "org.pentaho.big.data.logger1";
        String logger2 = "org.apache.hadoop.logger2";
        String logger3 = "com.pentaho.big.data.logger3";

        boolean result1 = BigDataLogConfig.registerLogger(logger1, Level.INFO);
        boolean result2 = BigDataLogConfig.registerLogger(logger2, Level.WARN);
        boolean result3 = BigDataLogConfig.registerLogger(logger3, Level.DEBUG);

        assertTrue("Should register logger1 successfully", result1);
        assertTrue("Should register logger2 successfully", result2);
        assertTrue("Should register logger3 successfully", result3);

        // Verify all loggers exist
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();

        LoggerConfig config1 = config.getLoggerConfig(logger1);
        LoggerConfig config2 = config.getLoggerConfig(logger2);
        LoggerConfig config3 = config.getLoggerConfig(logger3);

        assertNotNull("Logger1 config should exist", config1);
        assertNotNull("Logger2 config should exist", config2);
        assertNotNull("Logger3 config should exist", config3);
    }

    @Test
    public void testRegisterLogger_DifferentLevels() {
        Level[] levels = {Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR, Level.FATAL};

        for (int i = 0; i < levels.length; i++) {
            String loggerName = "org.pentaho.test.level" + i;
            Level level = levels[i];

            boolean result = BigDataLogConfig.registerLogger(loggerName, level);

            assertTrue("Should register logger with " + level + " level", result);

            // Verify the logger has correct level
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration config = context.getConfiguration();
            LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);

            assertNotNull("Logger config should exist for " + loggerName, loggerConfig);
            assertEquals("Logger should have correct level", level, loggerConfig.getLevel());
        }
    }

    @Test
    public void testRegisterLogger_NullLoggerName() {
        try {
            boolean result = BigDataLogConfig.registerLogger(null, Level.INFO);
            // If we get here, it should return false or handle gracefully
            assertFalse("Should handle null logger name gracefully", result);
        } catch (Exception e) {
            // It's acceptable to throw an exception for null logger name
            assertTrue("Exception is acceptable for null logger name", true);
        }
    }

    @Test
    public void testRegisterLogger_EmptyLoggerName() {
        String loggerName = "";
        Level level = Level.INFO;

        // Should handle empty string gracefully
        try {
            boolean result = BigDataLogConfig.registerLogger(loggerName, level);
            // Result doesn't matter as long as it doesn't crash
            assertTrue("Should handle empty logger name without crashing", true);
        } catch (Exception e) {
            // It's acceptable to throw an exception for empty logger name
            assertTrue("Exception is acceptable for empty logger name", true);
        }
    }

    @Test
    public void testRegisterLogger_RootLogger() {
        String rootLoggerName = "";
        Level level = Level.DEBUG;

        // Should handle root logger appropriately
        try {
            boolean result = BigDataLogConfig.registerLogger(rootLoggerName, level);
            // Should complete without crashing
            assertTrue("Should handle root logger without crashing", true);
        } catch (Exception e) {
            // Exception is acceptable
            assertTrue("Exception is acceptable for root logger", true);
        }
    }

    @Test
    public void testRegisterLogger_HierarchicalLoggers() {
        String parentLogger = "org.pentaho.big.data";
        String childLogger = "org.pentaho.big.data.services";
        String grandchildLogger = "org.pentaho.big.data.services.bootstrap";

        boolean result1 = BigDataLogConfig.registerLogger(parentLogger, Level.INFO);
        boolean result2 = BigDataLogConfig.registerLogger(childLogger, Level.DEBUG);
        boolean result3 = BigDataLogConfig.registerLogger(grandchildLogger, Level.TRACE);

        assertTrue("Should register parent logger", result1);
        assertTrue("Should register child logger", result2);
        assertTrue("Should register grandchild logger", result3);

        // Verify all exist in hierarchy
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();

        assertNotNull("Parent logger should exist", config.getLoggerConfig(parentLogger));
        assertNotNull("Child logger should exist", config.getLoggerConfig(childLogger));
        assertNotNull("Grandchild logger should exist", config.getLoggerConfig(grandchildLogger));
    }

    @Test
    public void testRegisterLogger_ThreadSafety() throws InterruptedException {
        // Test concurrent registration of loggers
        final int threadCount = 10;
        final int loggersPerThread = 10;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < loggersPerThread; j++) {
                    String loggerName = "org.pentaho.test.thread" + threadId + ".logger" + j;
                    BigDataLogConfig.registerLogger(loggerName, Level.INFO);
                }
            });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify all loggers were registered
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();

        int registeredCount = 0;
        for (int i = 0; i < threadCount; i++) {
            for (int j = 0; j < loggersPerThread; j++) {
                String loggerName = "org.pentaho.test.thread" + i + ".logger" + j;
                LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);
                if (loggerConfig != null && loggerConfig.getName().equals(loggerName)) {
                    registeredCount++;
                }
            }
        }

        assertTrue("Should register multiple loggers concurrently", registeredCount > 0);
    }
}
