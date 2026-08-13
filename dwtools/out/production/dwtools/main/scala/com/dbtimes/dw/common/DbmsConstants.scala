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
package com.dbtimes.dw.common

private[common] object DbmsConstants extends Enumeration {

  // Define the custom Val class to hold attributes
  protected[common] case class DbmsVal(
      driverClass: String,
      statementForMinLongValue: String,
      statementForMaxLongValue: String,
      methodsToGetLongValue: String ) extends Val(nextId)

  // Explicitly define values
  val SqlServer = DbmsVal(
    "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "( SELECT CAST( ISNULL( MIN( [<@PartitionColumn>] ), 0 ) AS BIGINT) AS Result FROM <@SourceTable> ) AS result",
    "( SELECT CAST( ISNULL( MAX( [<@PartitionColumn>] ), 1 ) AS BIGINT) AS Result FROM <@SourceTable> ) AS result",
    "getLong"
  );
  val Oracle = DbmsVal(
    "oracle.jdbc.driver.OracleDriver",
    "( SELECT CAST( NVL( MIN( <@PartitionColumn> ), 0 ) AS NUMBER(19,0) ) AS Result FROM <@SourceTable> )",
    "( SELECT CAST( NVL( MAX( <@PartitionColumn> ), 0 ) AS NUMBER(19,0) ) AS Result FROM <@SourceTable> )",
    "getDecimal.longValueExact"
  );

  // Import for convenient access
  import scala.language.implicitConversions
  implicit def dbmsValToDbms(dbmsVal: DbmsVal): Value = dbmsVal
}
