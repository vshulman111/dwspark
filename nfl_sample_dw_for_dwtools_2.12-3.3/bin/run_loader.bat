c:
cd %SPARK_HOME%\bin

set CURRENT_TIME=%date:~-4%%date:~-10,2%%date:~-7,2%_%time:~0,2%%time:~3,2%%time:~6,2%
set CURRENT_TIME=%CURRENT_TIME: =0%
set JOB_LOG_NAME=dw_loader_%CURRENT_TIME%

spark-submit^
 --conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"^
 --conf "spark.executor.extraJavaOptions=--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"^
 --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=file:///C:/dwspark/nfl_sample_dw_for_dwtools_2.12-3.3/code/NFLDataSourceLoader/src/main/resources/log4j2.properties -DlogFilename=C:/dwspark/nfl_sample_dw_for_dwtools_2.12-3.3/job_logs/%JOB_LOG_NAME%"^
 --conf "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=file:///C:/dwspark/nfl_sample_dw_for_dwtools_2.12-3.3/code/NFLDataSourceLoader/src/main/resources/log4j2.properties -DlogFilename=C:/dwspark/nfl_sample_dw_for_dwtools_2.12-3.3/job_logs/%JOB_LOG_NAME%"^
 --class com.dbtimes.nflsourceloader.SrcLoaderNFL^
 --master local C:\dwspark\nfl_sample_dw_for_dwtools_2.12-3.3\code\NFLDataSourceLoader\target\NFLDataSourceLoader-1.0-SNAPSHOT-jar-with-dependencies.jar nfl.json

pause
