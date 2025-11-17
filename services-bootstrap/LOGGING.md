# Big Data Plugin - Programmatic Logging Configuration

## Overview

The Big Data plugin uses a programmatic Log4j2 configuration system to ensure consistent and dedicated logging for all big-data components. This approach eliminates the need for manual XML configuration and provides automatic setup during plugin initialization.

## Key Components

### BigDataLogConfig

The `BigDataLogConfig` class provides programmatic configuration of Log4j2 appenders and loggers.

**Location:** `org.pentaho.big.data.services.bootstrap.BigDataLogConfig`

**Features:**
- Creates a dedicated `RollingFileAppender` for big-data logs
- Logs to: `logs/big-data.log`
- Automatic file rotation by date and size (50 MB max)
- Keeps up to 10 backup files
- Configures loggers for `com.pentaho.big.data.*` and `org.pentaho.big.data.*` packages
- Thread-safe initialization
- No XML configuration required

### Log File Configuration

- **File Name:** `logs/big-data.log`
- **File Pattern:** `logs/big-data_%d{yyyy-MM-dd}-%i.log`
- **Log Pattern:** `%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%c{1.}] <%t> %m%n`
- **Max File Size:** 50 MB
- **Max Backup Files:** 10
- **Rotation:** Daily and size-based

## Usage

### Getting a Logger

To use the big-data logger in your class, use one of the following methods:

```java
// Using class reference (recommended)
import org.pentaho.big.data.services.bootstrap.BigDataLogConfig;
import org.apache.logging.log4j.Logger;

public class MyBigDataClass {
    private static final Logger logger = BigDataLogConfig.getBigDataLogger(MyBigDataClass.class);
    
    public void doSomething() {
        logger.info("Doing something...");
        logger.debug("Debug information");
        logger.error("An error occurred", exception);
    }
}
```

```java
// Using logger name
private static final Logger logger = BigDataLogConfig.getBigDataLogger("com.pentaho.big.data.mycomponent");
```

### Initialization

The logging configuration is automatically initialized when:
1. The `BigDataCEServiceInitializerImpl` starts (calls `BigDataLogConfig.initializeBigDataLogging()`)
2. Any class calls `BigDataLogConfig.getBigDataLogger()` for the first time

The initialization is idempotent and thread-safe, so it can be called multiple times without issues.

## Architecture

### Initialization Flow

1. `BigDataPluginLifecycleListener` is triggered during Kettle lifecycle initialization
2. It calls `BigDataServicesInitializer.doInitialize()`
3. `BigDataCEServiceInitializerImpl.doInitialize()` explicitly calls `BigDataLogConfig.initializeBigDataLogging()`
4. The Log4j2 configuration is set up programmatically:
   - Creates the `big-data-appender` RollingFileAppender
   - Configures loggers for `com.pentaho.big.data` and `org.pentaho.big.data` packages
   - Updates the Log4j2 context

### Updated Classes

The following classes have been updated to use the programmatic logger:

1. **BigDataCEServiceInitializerImpl** - Main service initializer
2. **BigDataPluginLifecycleListener** - Lifecycle listener
3. **BigDataLogConfig** - Logging configuration utility

## Benefits

1. **No XML Configuration Required** - All configuration is done programmatically
2. **Consistent Logging** - All big-data components use the same appender
3. **Automatic Setup** - No manual configuration needed
4. **Centralized Management** - Easy to modify logging behavior in one place
5. **Type-Safe** - Compile-time checking of logger names
6. **Dedicated Log Files** - Separates big-data logs from other Pentaho logs

## Troubleshooting

### Logging Not Working

If logging is not working as expected:

1. Check that `BigDataLogConfig.initializeBigDataLogging()` is called during initialization
2. Verify that the `logs` directory exists and is writable
3. Check Log4j2 configuration status messages in the console
4. Ensure your package name starts with `com.pentaho.big.data` or `org.pentaho.big.data`

### Checking Initialization Status

```java
if (BigDataLogConfig.isInitialized()) {
    logger.info("Big Data logging is initialized");
} else {
    logger.warn("Big Data logging is NOT initialized");
}
```

## API Reference

### BigDataLogConfig Methods

- `static void initializeBigDataLogging()` - Initialize the logging configuration (idempotent)
- `static Logger getBigDataLogger(Class<?> clazz)` - Get a logger for the specified class
- `static Logger getBigDataLogger(String name)` - Get a logger with the specified name
- `static boolean isInitialized()` - Check if logging has been initialized

## Migration Guide

If you have existing code using `LogManager.getLogger()`:

**Before:**
```java
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

private static final Logger logger = LogManager.getLogger(MyClass.class);
```

**After:**
```java
import org.pentaho.big.data.services.bootstrap.BigDataLogConfig;
import org.apache.logging.log4j.Logger;

private static final Logger logger = BigDataLogConfig.getBigDataLogger(MyClass.class);
```

## Future Enhancements

Potential future improvements:
- Support for different log levels per component
- Dynamic log level changes at runtime
- Additional appenders (e.g., for remote logging)
- Integration with Pentaho's centralized logging infrastructure
