@echo off
REM ***************************************
REM   BATCH SCRIPT TO START ADMIN CONSOLE
REM ***************************************
set CLASSPATH=.;resources
FOR %%F IN (lib\*.jar) DO call :updateClassPath %%F

goto :startjava

:updateClassPath
set CLASSPATH=%CLASSPATH%;%1
goto :eof

:startjava
call java -Xmx512M -XX:PermSize=64M -XX:MaxPermSize=128M -cp %CLASSPATH%  org.pentaho.commons.metadata.mqleditor.editor.SwingDatasourceEditor
