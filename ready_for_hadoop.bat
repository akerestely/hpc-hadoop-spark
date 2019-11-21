rem set this to your jdk install dir
set JAVA_HOME=C:\Progra~1\Java\jdk1.8.0_221

rem this script should be at same level with hadoop intall directory for following path to work
rem otherwise set to hadoop folder
set HADOOP_HOME=%CD%\hadoop

set HADOOP_BIN=%HADOOP_HOME%\bin
set HADOOP_SBIN=%HADOOP_HOME%\sbin

set PATH="%JAVA_HOME%\bin";"%HADOOP_HOME%";"%HADOOP_BIN%";"%HADOOP_SBIN%";%PATH%

call hadoop\etc\hadoop\hadoop-env.cmd