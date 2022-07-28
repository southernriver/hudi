package org.apache.spark.sql.hudi.catalog

import com.google.common.collect.Lists
import org.apache.hudi.HoodieSparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.List


class HoodieBatchScanBuilder(writeOptions: CaseInsensitiveStringMap,
                             hoodieCatalogTable: HoodieCatalogTable,
                             spark: SparkSession)
    extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

  private var filterExpressions: Option[Expression] = None

  private var filterArrays: Array[Filter] = new Array[Filter](0)

  override def build(): Scan = ???

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val pushed: util.List[Filter] = Lists.newArrayListWithExpectedSize(filters.length)

    this.filterExpressions = HoodieSparkUtils.convertToCatalystExpression(filters, hoodieCatalogTable.tableSchema)
    for (elem <- filters) {
      pushed.add(elem)
    }
    this.filterArrays = pushed.toArray(new Array[Filter](0))
    filters
  }

  override def pushedFilters(): Array[Filter] = {
    filterArrays
  }

  override def pruneColumns(structType: StructType): Unit = {


  }
}
