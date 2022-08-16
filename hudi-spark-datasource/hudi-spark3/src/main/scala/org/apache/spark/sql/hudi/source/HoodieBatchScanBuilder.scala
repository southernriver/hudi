package org.apache.spark.sql.hudi.source

import org.apache.hudi.DataSourceReadOptions.QUERY_TYPE
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.{DataSourceOptionsHelper, DefaultSource, EmptyRelation, HoodieBaseRelation, HoodieBootstrapRelation, HoodieEmptyRelation, HoodieSparkUtils, IncrementalRelation, MergeOnReadSnapshotRelation}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation
import org.apache.spark.sql.execution.streaming.ConsoleRelation
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.util


class HoodieBatchScanBuilder(spark: SparkSession,
                             hoodieCatalogTable: HoodieCatalogTable,
                             options: Map[String, String])
    extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns  {
  @transient lazy val hadoopConf = {
    // Hadoop Configurations are case sensitive.
    spark.sessionState.newHadoopConfWithOptions(options)
  }

  private var filterExpressions: Option[Expression] = None

  private var filterArrays: Array[Filter] = Array.empty

  // TODO generate exepected schema
  private val expectedSchema = hoodieCatalogTable.tableSchema

  override def build(): Scan = {
    println("HoodieBatchScanBuilder.scan")
    val relation = new DefaultSource().createRelation(new SQLContext(spark), options)
    relation match {
      case HadoopFsRelation(location, partitionSchema, dataSchema, _, _, options) =>
        // TODO support SupportsRuntimeFiltering && PartitonPushDown
        val selectedPartitions = location.listFiles(Seq.empty, filterExpressions.toList)
        SparkBatchScan(spark, selectedPartitions, dataSchema, partitionSchema,
            expectedSchema, filterArrays, options, hadoopConf)
      case _ =>
        val isBootstrappedTable = hoodieCatalogTable.metaClient.getTableConfig.getBootstrapBasePath.isPresent
        val tableType = hoodieCatalogTable.metaClient.getTableType
        val parameters = DataSourceOptionsHelper.parametersWithReadDefaults(options)
        val queryType = parameters(QUERY_TYPE.key)
        throw new HoodieException("Hoodie do not support read with DataSource V2 for (tableType, queryType, " +
          "isBootstrappedTable) = (" + tableType + "," + queryType + "," + isBootstrappedTable + ")")
    }
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filterExpressions = HoodieSparkUtils.convertToCatalystExpression(filters, expectedSchema)
    this.filterArrays = filters
    filters
  }

  override def pushedFilters(): Array[Filter] = {
    filterArrays
  }

  override def pruneColumns(structType: StructType): Unit = {
    // TODO support prune columns.
  }
}
