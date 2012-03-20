package org.pentaho.di.job.entries.hadoopjobexecutor;

import org.pentaho.di.i18n.BaseMessages;

import java.io.FileDescriptor;
import java.net.InetAddress;
import java.security.Permission;

/**
 * A security manager that prevents JVM halts (e.g. {@link System#exit(int)}).
 */
public class NoExitSecurityManager extends SecurityManager {
  private SecurityManager decorated;

  public static class NoJvmExitSecurityException extends SecurityException {
    private int status;

    public NoJvmExitSecurityException(int status, String message) {
      super(message);
      this.status = status;
    }
    
    public int getStatus() {
      return status;
    }
  }

  public NoExitSecurityManager(SecurityManager decorated) {
    this.decorated = decorated;
  }

  @Override
  public void checkExit(int status) {
    if (decorated != null) {
      decorated.checkExit(status);
    }
    throw new NoJvmExitSecurityException(status, BaseMessages.getString(getClass(), "NoSystemExit"));
  }

  @Override
  public void checkExec(String cmd) {
    if (decorated != null) {
      decorated.checkExec(cmd);
    }
  }

  @Override
  public void checkLink(String lib) {
    if (decorated != null) {
      decorated.checkLink(lib);
    }
  }

  @Override
  public void checkRead(FileDescriptor fd) {
    if (decorated != null) {
      decorated.checkRead(fd);
    }
  }

  @Override
  public void checkRead(String file) {
    if (decorated != null) {
      decorated.checkRead(file);
    }
  }

  @Override
  public void checkRead(String file, Object context) {
    if (decorated != null) {
      decorated.checkRead(file, context);
    }
  }

  @Override
  public void checkWrite(FileDescriptor fd) {
    if (decorated != null) {
      decorated.checkWrite(fd);
    }
  }

  @Override
  public void checkWrite(String file) {
    if (decorated != null) {
      decorated.checkWrite(file);
    }
  }

  @Override
  public void checkDelete(String file) {
    if (decorated != null) {
      decorated.checkDelete(file);
    }
  }

  @Override
  public void checkConnect(String host, int port) {
    if (decorated != null) {
      decorated.checkConnect(host, port);
    }
  }

  @Override
  public void checkConnect(String host, int port, Object context) {
    if (decorated != null) {
      decorated.checkConnect(host, port, context);
    }
  }

  @Override
  public void checkListen(int port) {
    if (decorated != null) {
      decorated.checkListen(port);
    }
  }

  @Override
  public void checkAccept(String host, int port) {
    if (decorated != null) {
      decorated.checkAccept(host, port);
    }
  }

  @Override
  public void checkMulticast(InetAddress maddr) {
    if (decorated != null) {
      decorated.checkMulticast(maddr);
    }
  }

  @Override
  @Deprecated
  public void checkMulticast(InetAddress maddr, byte ttl) {
    if (decorated != null) {
      decorated.checkMulticast(maddr, ttl);
    }
  }

  @Override
  public void checkPropertiesAccess() {
    if (decorated != null) {
      decorated.checkPropertiesAccess();
    }
  }

  @Override
  public void checkPropertyAccess(String key) {
    if (decorated != null) {
      decorated.checkPropertyAccess(key);
    }
  }

  @Override
  public boolean checkTopLevelWindow(Object window) {
    if (decorated != null) {
      return decorated.checkTopLevelWindow(window);
    }
    return true; // Everything is allowed to put up top-level windows
  }

  @Override
  public void checkPrintJobAccess() {
    if (decorated != null) {
      decorated.checkPrintJobAccess();
    }
  }

  @Override
  public void checkSystemClipboardAccess() {
    if (decorated != null) {
      decorated.checkSystemClipboardAccess();
    }
  }

  @Override
  public void checkAwtEventQueueAccess() {
    if (decorated != null) {
      decorated.checkAwtEventQueueAccess();
    }
  }

  @Override
  public void checkPackageAccess(String pkg) {
    if (decorated != null) {
      decorated.checkPackageAccess(pkg);
    }
  }

  @Override
  public void checkPackageDefinition(String pkg) {
    if (decorated != null) {
      decorated.checkPackageDefinition(pkg);
    }
  }

  @Override
  public void checkSetFactory() {
    if (decorated != null) {
      decorated.checkSetFactory();
    }
  }

  @Override
  public void checkMemberAccess(Class<?> clazz, int which) {
    if (decorated != null) {
      decorated.checkMemberAccess(clazz, which);
    }
  }

  @Override
  public void checkSecurityAccess(String target) {
    if (decorated != null) {
      decorated.checkSecurityAccess(target);
    }
  }

  @Override
  public void checkPermission(Permission perm) {
    if (decorated != null) {
      decorated.checkPermission(perm);
    }
  }

  @Override
  public void checkPermission(Permission perm, Object context) {
    if (decorated != null) {
      decorated.checkPermission(perm, context);
    }
  }

  @Override
  public void checkCreateClassLoader() {
    if (decorated != null) {
      decorated.checkCreateClassLoader();
    }
  }

  @Override
  public void checkAccess(Thread t) {
    if (decorated != null) {
      decorated.checkAccess(t);
    }
  }

  @Override
  public void checkAccess(ThreadGroup g) {
    if (decorated != null) {
      decorated.checkAccess(g);
    }
  }
}
