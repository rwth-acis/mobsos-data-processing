cd ..
set BASE=%CD%
set CLASSPATH="%BASE%/lib/*;"

java -cp %CLASSPATH% i5.las2peer.tools.L2pNodeLauncher -w -p 9010 startService('i5.las2peer.services.monitoring.processing.MonitoringDataProcessingService','MDPSPass') interactive
pause