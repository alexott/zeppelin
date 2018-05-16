/*
 * Forked from original Zeppelin code by Alexey Ott. All made changes are
 * copyrighted by DataStax, 2018
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.cassandra

import com.datastax.driver.core._
import com.datastax.driver.dse.DseSession
import org.apache.zeppelin.cassandra.TextBlockHierarchy._
import org.apache.zeppelin.interpreter.InterpreterException

import scala.collection.JavaConverters._

/**
 * Enhance the Java driver session
 * with special statements
 * to describe schema
 */
object EnhancedSession {

  val clusterDisplay = DisplaySystem.ClusterDisplay
  val keyspaceDisplay = DisplaySystem.KeyspaceDisplay
  val tableDisplay = DisplaySystem.TableDisplay
  val udtDisplay = DisplaySystem.UDTDisplay
  val functionDisplay = DisplaySystem.FunctionDisplay
  val aggregateDisplay = DisplaySystem.AggregateDisplay
  val materializedViewDisplay = DisplaySystem.MaterializedViewDisplay
  val helpDisplay = DisplaySystem.HelpDisplay
  private val noResultDisplay = DisplaySystem.NoResultDisplay


  val HTML_MAGIC = "%html \n"
  val TEXT_MAGIC = "%text \n"

  val displayNoResult: String = HTML_MAGIC + noResultDisplay.formatNoResult

  def displayExecutionStatistics(query: String, execInfo: ExecutionInfo): String = {
    HTML_MAGIC + noResultDisplay.noResultWithExecutionInfo(query, execInfo)
  }

  private def execute(session: Session, describeCluster: DescribeClusterCmd): String = {
    val metaData = session.getCluster.getMetadata
    HTML_MAGIC + clusterDisplay.formatClusterOnly(describeCluster.statement, metaData)
  }

  private def execute(session: Session, describeKeyspaces: DescribeKeyspacesCmd): String = {
    val metaData = session.getCluster.getMetadata
    HTML_MAGIC + clusterDisplay.formatClusterContent(describeKeyspaces.statement, metaData)
  }

  private def execute(session: Session, describeTables: DescribeTablesCmd): String = {
    val metadata: Metadata = session.getCluster.getMetadata
    HTML_MAGIC + clusterDisplay.formatAllTables(describeTables.statement,metadata)
  }

  private def execute(session: Session, describeKeyspace: DescribeKeyspaceCmd): String = {
    val keyspace: String = describeKeyspace.keyspace
    val metadata: Option[KeyspaceMetadata] = Option(session.getCluster.getMetadata.getKeyspace(keyspace))
    metadata match {
      case Some(ksMeta) => HTML_MAGIC + keyspaceDisplay.formatKeyspaceContent(describeKeyspace.statement, ksMeta,
        session.getCluster.getConfiguration.getCodecRegistry)
      case None => throw new InterpreterException(s"Cannot find keyspace $keyspace")
    }
  }

  private def execute(session: Session, describeTable: DescribeTableCmd): String = {
    val metaData = session.getCluster.getMetadata
    val tableName: String = describeTable.table
    val keyspace: String = describeTable.keyspace.orElse(Option(session.getLoggedKeyspace)).getOrElse("system")

    Option(metaData.getKeyspace(keyspace)).flatMap(ks => Option(ks.getTable(tableName))) match {
      case Some(tableMeta) => HTML_MAGIC + tableDisplay.format(describeTable.statement, tableMeta, true)
      case None => throw new InterpreterException(s"Cannot find table $keyspace.$tableName")
    }
  }

  private def execute(session: Session, describeUDT: DescribeTypeCmd): String = {
    val metaData = session.getCluster.getMetadata
    val keyspace: String = describeUDT.keyspace.orElse(Option(session.getLoggedKeyspace)).getOrElse("system")
    val udtName: String = describeUDT.udtName

    Option(metaData.getKeyspace(keyspace)).flatMap(ks => Option(ks.getUserType(udtName))) match {
      case Some(userType) => HTML_MAGIC + udtDisplay.format(describeUDT.statement, userType, true)
      case None => throw new InterpreterException(s"Cannot find type $keyspace.$udtName")
    }
  }

  private def execute(session: Session, describeUDTs: DescribeTypesCmd): String = {
    val metadata: Metadata = session.getCluster.getMetadata
    HTML_MAGIC + clusterDisplay.formatAllUDTs(describeUDTs.statement, metadata)
  }

  private def execute(session: Session, describeFunction: DescribeFunctionCmd): String = {
    val metaData = session.getCluster.getMetadata
    val keyspaceName: String = describeFunction.keyspace.orElse(Option(session.getLoggedKeyspace)).getOrElse("system")
    val functionName: String = describeFunction.function;

    Option(metaData.getKeyspace(keyspaceName)) match {
      case Some(keyspace) => {
        val functionMetas: List[FunctionMetadata] = keyspace.getFunctions.asScala.toList
          .filter(func => func.getSimpleName.toLowerCase == functionName.toLowerCase)

        if(functionMetas.isEmpty) {
          throw new InterpreterException(s"Cannot find function ${keyspaceName}.$functionName")
        } else {
          HTML_MAGIC + functionDisplay.format(describeFunction.statement, functionMetas, true)
        }
      }
      case None => throw new InterpreterException(s"Cannot find function ${keyspaceName}.$functionName")
    }
  }

  private def execute(session: Session, describeFunctions: DescribeFunctionsCmd): String = {
    val metadata: Metadata = session.getCluster.getMetadata
    HTML_MAGIC + clusterDisplay.formatAllFunctions(describeFunctions.statement, metadata)
  }

  private def execute(session: Session, describeAggregate: DescribeAggregateCmd): String = {
    val metaData = session.getCluster.getMetadata
    val keyspaceName: String = describeAggregate.keyspace.orElse(Option(session.getLoggedKeyspace)).getOrElse("system")
    val aggregateName: String = describeAggregate.aggregate;

    Option(metaData.getKeyspace(keyspaceName)) match {
      case Some(keyspace) => {
        val aggMetas: List[AggregateMetadata] = keyspace.getAggregates.asScala.toList
          .filter(agg => agg.getSimpleName.toLowerCase == aggregateName.toLowerCase)

        if(aggMetas.isEmpty) {
          throw new InterpreterException(s"Cannot find aggregate ${keyspaceName}.$aggregateName")
        } else {
          HTML_MAGIC + aggregateDisplay.format(describeAggregate.statement, aggMetas, true,
            session
            .getCluster
            .getConfiguration
            .getCodecRegistry)
        }
      }
      case None => throw new InterpreterException(s"Cannot find aggregate ${keyspaceName}.$aggregateName")
    }
  }

  private def execute(session: Session, describeAggregates: DescribeAggregatesCmd): String = {
    val metadata: Metadata = session.getCluster.getMetadata
    HTML_MAGIC + clusterDisplay.formatAllAggregates(describeAggregates.statement, metadata)
  }

  private def execute(session: Session, describeMV: DescribeMaterializedViewCmd): String = {
    val metaData = session.getCluster.getMetadata
    val keyspaceName: String = describeMV.keyspace.orElse(Option(session.getLoggedKeyspace)).getOrElse("system")
    val viewName: String = describeMV.view

    Option(metaData.getKeyspace(keyspaceName)) match {
      case Some(keyspace) => {
        val viewMeta: Option[MaterializedViewMetadata] = Option(keyspace.getMaterializedView(viewName))
        viewMeta match {
          case Some(vMeta) => HTML_MAGIC + materializedViewDisplay.format(describeMV.statement, vMeta, true)
          case None => throw new InterpreterException(s"Cannot find materialized view ${keyspaceName}.$viewName")
        }
      }
      case None => throw new InterpreterException(s"Cannot find materialized view ${keyspaceName}.$viewName")
    }
  }

  private def execute(session: Session, describeMVs: DescribeMaterializedViewsCmd): String = {
    val metadata: Metadata = session.getCluster.getMetadata
    HTML_MAGIC + clusterDisplay.formatAllMaterializedViews(describeMVs.statement, metadata)
  }

  private def execute(helpCmd: HelpCmd): String = {
    HTML_MAGIC + helpDisplay.formatHelp()
  }

  private def execute(session: Session, describeSearchIndex: DescribeSearchIndexCmd): String = {
    val res = session.execute(describeSearchIndex.statement)
    val r1 = res.one()
    if (r1 == null) {
      throw new InterpreterException(s"There is no search index on ${describeSearchIndex.keyspace.get}.${describeSearchIndex.table}")
    } else {
      val resource = r1.getString("resource")
      TEXT_MAGIC + resource
    }
  }

// TODO(alex): think how to make it's more generic, without match
  def execute(session: Session, st: Any): Any = {
    st match {
      case x:DescribeClusterCmd => execute(session, x)
      case x:DescribeKeyspaceCmd => execute(session, x)
      case x:DescribeKeyspacesCmd => execute(session, x)
      case x:DescribeTableCmd => execute(session, x)
      case x:DescribeTablesCmd => execute(session, x)
      case x:DescribeTypeCmd => execute(session, x)
      case x:DescribeTypesCmd => execute(session, x)
      case x:DescribeFunctionCmd => execute(session, x)
      case x:DescribeFunctionsCmd => execute(session, x)
      case x:DescribeAggregateCmd => execute(session, x)
      case x:DescribeAggregatesCmd => execute(session, x)
      case x:DescribeMaterializedViewCmd => execute(session, x)
      case x:DescribeMaterializedViewsCmd => execute(session, x)
      case x:DescribeSearchIndexCmd => execute(session, x)
      case x:HelpCmd => execute(session, x)
      case x:Statement => session.execute(x)
      case _ => throw new InterpreterException(s"Cannot execute statement '$st' of type ${st.getClass}")
    }
  }
}
