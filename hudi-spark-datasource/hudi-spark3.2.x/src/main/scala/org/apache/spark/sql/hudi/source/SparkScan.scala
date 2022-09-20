package org.apache.spark.sql.hudi.source

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.read.{Batch, Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.sources.{Filter, In}
import org.apache.spark.sql.types.StructType

case class SparkScan(spark: SparkSession,
                     hoodieTableName: String,
                     selectedPartitions: Seq[PartitionDirectory],
                     tableSchema: StructType,
                     partitionSchema: StructType,
                     requiredSchema: StructType,
                     filters: Seq[Filter],
                     options: Map[String, String],
                     @transient hadoopConf: Configuration)
  extends SparkBatch (
    spark,
    selectedPartitions,
    partitionSchema,
    requiredSchema,
    filters,
    options,
    hadoopConf) with Scan with SupportsRuntimeFiltering {

  override def readSchema(): StructType = {
    requiredSchema
  }

  override def toBatch: Batch = this

  override def description(): String = {
    hoodieTableName + ", PushedFilters: " + filters.mkString("[", ", ", "], ")
  }

  override def filterAttributes(): Array[NamedReference] = {
    val scanFields = readSchema().fields.map(_.name).toSet

    val namedReference = partitionSchema.fields.filter(field => scanFields.contains(field.name))
      .map(field => Expressions.column(field.name))
    namedReference
  }

  override def filter(filters: Array[Filter]): Unit = {
    // TODO need to filter out irrelevant data files
  }

}
