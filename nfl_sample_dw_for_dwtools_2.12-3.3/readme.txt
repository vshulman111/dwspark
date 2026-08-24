Copyright 2026 Victor Shulman
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law  or agreed to in writing, software distributed under the License is distributed on an 
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
See the License for the specific language governing permissions and limitations under the License.
--------------------------------------------------------------------------------------------------

This sample implementation uses public NFL data.
See Dimensional Model design in NFLDataWarehouse.docx

The paths in this implementation is for Windows. If running on UNIX the following components would need to change
- file paths in application json configuration files
	- C:\dwspark\nfl_sample_dw_for_dwtools_2.12-3.3\code\NFLDataSourceLoader\src\main\resources\nfl.json
	- C:\dwspark\nfl_sample_dw_for_dwtools_2.12-3.3\code\NFLDataSourceRecon\src\main\resources\nfl_recon.json
	- C:\dwspark\nfl_sample_dw_for_dwtools_2.12-3.3\code\NFLDwEtl\src\main\resources\nfl_initial.json
- scripts in C:\dwspark\nfl_sample_dw_for_dwtools_2.12-3.3\bin directory

To build the project 
- create top level directory C:\dwspark
- pull folder nfl_sample_dw_for_dwtools_2.12-3.3 with all sub-folders
- use Maven for all three applications in ..\code directory. For example,
	cd C:\dwspark\nfl_sample_dw_for_dwtools_2.12-3.3\code\NFLDataSourceLoader
	mvn clean package

to run locally 
- cd C:\dwspark\nfl_sample_dw_for_dwtools_2.12-3.3\bin
- use run commands