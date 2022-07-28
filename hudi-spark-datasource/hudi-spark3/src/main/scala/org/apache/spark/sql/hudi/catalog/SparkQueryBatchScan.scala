package org.apache.spark.sql.hudi.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportPartitioning, SupportsReportStatistics, SupportsRuntimeFiltering}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class SparkQueryBatchScan(
    var spark: SparkSession,
    var table: HoodieInternalV2Table,
    readSchema: StructType,
    tableSchema: StructType) extends Scan with Batch with SupportsRuntimeFiltering with SupportsReportStatistics
  with SupportsReportPartitioning {
  override def planInputPartitions(): Array[InputPartition] = ???

  override def createReaderFactory(): PartitionReaderFactory = ???

  /*
  {
    val tableBroadcast = spark.sparkContext.broadcast(table)
    val readTasks = new InputPartition {}
  }
   */



  override def filterAttributes(): Array[NamedReference] = ???

  override def filter(filters: Array[Filter]): Unit = ???

  override def estimateStatistics(): Statistics = ???

  override def outputPartitioning(): Partitioning = ???
}

class ReadTask extends InputPartition with Serializable{


}


