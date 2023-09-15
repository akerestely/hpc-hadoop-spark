C:\Progra~1\Java\jdk1.8.0_221\bin\javac -version
if errorlevel 1 (
    rem required java is missing, so intall
    jdk-8u221-windows-x64.exe /s ADDLOCAL="ToolsFeature,SourceFeature"
)

mkdir temp

rem download and extract hadoop
powershell -Command "Start-BitsTransfer -Source https://www-us.apache.org/dist/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz -Destination temp\hadoop.tar.gz"
if errorlevel 1 exit /b %errorlevel%

mkdir hadoop_temp
tar -xzf temp\hadoop.tar.gz -C hadoop_temp --totals
for /d %%a in ("hadoop_temp\*") do ren %%a hadoop
move hadoop_temp\hadoop .
rmdir hadoop_temp
rem copy a jar for resource manager to work
xcopy /Y /I hadoop\share\hadoop\yarn\timelineservice\hadoop-yarn-server-timelineservice-3.1.2.jar hadoop\share\hadoop\yarn\

rem download and extract fix for hadoop on windows
powershell -Command "Invoke-WebRequest https://github.com/s911415/apache-hadoop-3.1.0-winutils/archive/master.zip -OutFile temp\hadoop_fix.zip"
powershell -Command "Expand-Archive -Force temp\hadoop_fix.zip temp\hadoop_fix"
rmdir hadoop\bin /s /q
for /d %%a in ("temp\hadoop_fix\*") do xcopy /Y /I %%a\bin hadoop\bin

rmdir /S /Q temp

rem create hdfs storage location
mkdir hdfs\namenode
mkdir hdfs\datanode

call ready_for_hadoop.bat

rem edit configuration files
powershell -File .\edit_configs.ps1 %cd%\hdfs\namenode %cd%\hdfs\datanode

echo Y| hdfs namenode -format

rem Name Node:
start http://localhost:9870
rem Data Node:
start http://localhost:9864 
rem Resource Manager:
start http://localhost:8088
rem Node Manager:
start http://localhost:8042