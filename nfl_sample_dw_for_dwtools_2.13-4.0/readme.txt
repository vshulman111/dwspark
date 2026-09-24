Copyright 2026 Victor Shulman
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law  or agreed to in writing, software distributed under the License is distributed on an 
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
See the License for the specific language governing permissions and limitations under the License.
--------------------------------------------------------------------------------------------------

This sample implementation uses public NFL data.
See Dimensional Model design in NFLDataWarehouse.docx


1. Running locally on Windows
-----------------------------
The paths in json configurations use c: drive.
	- C:\dwspark\nfl_sample_dw_for_dwtools_2.13-4.0\code\NFLDataSourceLoader\src\main\resources\nfl.json
	- C:\dwspark\nfl_sample_dw_for_dwtools_2.13-4.0\code\NFLDataSourceRecon\src\main\resources\nfl_recon.json
	- C:\dwspark\nfl_sample_dw_for_dwtools_2.13-4.0\code\NFLDwEtl\src\main\resources\nfl_initial.json

To build the project 
- create top level directory C:\dwspark
- pull folder nfl_sample_dw_for_dwtools_2.13-4.0 with all sub-folders from github.com/vshulman111/dwspark.git
- use Maven for all three applications in ..\code directory. For example,
	cd C:\dwspark\nfl_sample_dw_for_dwtools_2.13-4.0\code\NFLDataSourceLoader
	mvn clean package

to run locally 
- cd C:\dwspark\nfl_sample_dw_for_dwtools_2.13-4.0\bin
- use run commands

2. Running on Databricks/Azure
------------------------------
To build the project add -P cluster to maven build command - that will create a jar file without spark libraries that are provided by Databricks environment
- use Maven for all three applications in ..\code directory. For example,
	mvn clean package -P cluster

- The paths in json configurations use dbfs: location
- Sample configurations for running jobs
	- C:\dwspark\nfl_sample_dw_for_dwtools_2.13-4.0\code\NFLDataSourceLoader\src\main\resources\nfl_dbr_azure.json
	- C:\dwspark\nfl_sample_dw_for_dwtools_2.13-4.0\code\NFLDataSourceRecon\src\main\resources\nfl_recon_dbr_azure.json
	- C:\dwspark\nfl_sample_dw_for_dwtools_2.13-4.0\code\NFLDwEtl\src\main\resources\nfl_initial_dbr_azure.json

- Make sure the there is no spark.stop() call in your application as it causes Spark to through an Exception - Spark is managed outside of application code 
- Do not initialize spark in the code, do it in Spark Cluster->Advanced->Spark->Spark config  
