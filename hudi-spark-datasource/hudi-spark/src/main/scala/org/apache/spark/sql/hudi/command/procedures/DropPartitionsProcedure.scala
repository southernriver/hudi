/*
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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.ValidationUtils.checkArgument
import org.apache.hudi.{AvroConversionUtils, HoodieCLIUtils, HoodieFileIndex, HoodieSparkSqlWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.HoodieCatalystExpressionUtils.{resolveExpr, splitPartitionAndDataPredicates}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}

import java.util.function.Supplier

class DropPartitionsProcedure extends BaseProcedure
  with ProcedureBuilder
  with PredicateHelper
  with ProvidesHoodieConfig
  with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "path", DataTypes.StringType, None),
    ProcedureParameter.optional(2, "predicate", DataTypes.StringType, None),
    ProcedureParameter.optional(3, "selected_partitions", DataTypes.StringType, None)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val predicate = getArgValueOrDefault(args, PARAMETERS(2))
    val parts = getArgValueOrDefault(args, PARAMETERS(3))

    val basePath: String = getBasePath(tableName, tablePath)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val selectedPartitions: String = (parts, predicate) match {
      case (_, Some(p)) => prunePartition(metaClient, p.asInstanceOf[String])
      case (Some(o), _) => o.asInstanceOf[String]
      case _ => ""
    }

    // Set the cleaner policy to lazy.
    spark.sql("set hoodie.cleaner.policy.failed.writes=LAZY")

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, metaClient.getTableConfig.getTableName)
    if (selectedPartitions.nonEmpty) {
      val parameters = buildHoodieDropPartitionsConfig(sparkSession, hoodieCatalogTable, selectedPartitions)
      HoodieSparkSqlWriter.write(
        sparkSession.sqlContext,
        SaveMode.Append,
        parameters,
        sparkSession.emptyDataFrame)
      logInfo(s"Drop partitions : $selectedPartitions")
      Seq(Row(true, selectedPartitions))
    } else {
      logInfo("No partition to drop")
      Seq(Row(false, selectedPartitions))
    }
  }

  override def build: Procedure = new DropPartitionsProcedure()

  def prunePartition(metaClient: HoodieTableMetaClient, predicate: String): String = {
    val options = Map(QUERY_TYPE.key() -> QUERY_TYPE_SNAPSHOT_OPT_VAL, "path" -> metaClient.getBasePath)
    val hoodieFileIndex = HoodieFileIndex(sparkSession, metaClient, None, options,
      FileStatusCache.getOrCreate(sparkSession))

    // Resolve partition predicates
    val schemaResolver = new TableSchemaResolver(metaClient)
    val tableSchema = AvroConversionUtils.convertAvroSchemaToStructType(schemaResolver.getTableAvroSchema)
    val condition = resolveExpr(sparkSession, predicate, tableSchema)
    val partitionColumns = metaClient.getTableConfig.getPartitionFields.orElse(Array[String]())
    val (partitionPredicates, dataPredicates) = splitPartitionAndDataPredicates(
      sparkSession, splitConjunctivePredicates(condition).toArray, partitionColumns)
    checkArgument(dataPredicates.isEmpty, "Only partition predicates are allowed")

    // Get all partitions and prune partition by predicates
    val prunedPartitions = hoodieFileIndex.getPartitionPaths(partitionPredicates)
    prunedPartitions.map(path => path.getPath).toSet.mkString(",")
  }
}

object DropPartitionsProcedure {
  val NAME = "drop_partitions"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new DropPartitionsProcedure
  }
}
