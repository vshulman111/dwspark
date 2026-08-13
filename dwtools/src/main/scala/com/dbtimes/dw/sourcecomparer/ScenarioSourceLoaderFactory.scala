/*
--	Copyright (c) 2007 - 2021 by Victor Shulman.
--	Written by Victor Shulman.  Not derived from licensed software.
--
--	Permission is granted to anyone to use this software for any
--	purpose on any computer system, and to redistribute it in any way,
--	subject to the following restrictions:
--
--	1. The author is not responsible for the consequences of use of
--		this software, no matter how awful, even if they arise
--		from defects in it.
--
--	2. The origin of this software must not be misrepresented, either
--		by explicit claim or by omission.
--
--	3. This notice must not be removed or altered.
*/
package com.dbtimes.dw.sourcecomparer

private[sourcecomparer] object ScenarioSourceLoaderFactory {

  private[sourcecomparer] def getScenarioSourceLoader(scenario: ScenarioConfig, isLeftSource: Boolean ): ScenarioSourceLoader = {
    if (scenario.getIsFileSourceCsv( isLeftSource ) )
      new LoaderOfCsvFileSource(scenario, isLeftSource)
    else if (scenario.getIsFileSourceParquet( isLeftSource ) )
      new LoaderOfParquetFileSource(scenario, isLeftSource)
    else if (scenario.getIsDbmsSourceSqlServer( isLeftSource ) )
      new LoaderOfSqlServerSource(scenario, isLeftSource)
    else if (scenario.getIsDbmsSourceOracle( isLeftSource ) )
      new LoaderOfOracleSource(scenario, isLeftSource)
    else if (scenario.getIsDbmsSourceMongoDb( isLeftSource ) )
      new LoaderOfMongDbSource(scenario, isLeftSource)
    else
      throw new RuntimeException("Loader ERROR: unhandled source type in compare scenario " + scenario.getName)
  }
}
