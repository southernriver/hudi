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

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.index.HoodieIndexUtils
import org.apache.hudi.index.bucket.BucketIdentifier
import org.apache.hudi.table.HoodieSparkTable
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.types._

import java.util.function.Supplier
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.jdk.CollectionConverters.seqAsJavaListConverter

class CheckBucketFileIdProcedure extends BaseProcedure
  with ProcedureBuilder
  with PredicateHelper
  with ProvidesHoodieConfig
  with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType, None))

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("fileId", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))

    val basePath: String = getBasePath(tableName)
    val client = createHoodieClient(jsc, basePath)
    val config = client.getConfig
    val context = client.getEngineContext
    val table = HoodieSparkTable.create(config, context)

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, tableName.get.asInstanceOf[String])
    val partitionPaths = hoodieCatalogTable.getPartitionPaths.toList.asJava

    val javaRdd: JavaRDD[String] = jsc.parallelize(partitionPaths, partitionPaths.size())
    javaRdd.rdd.map(part => HoodieIndexUtils.getLatestFileSlicesForPartition(part, table))
      .flatMap(_.toList)
      .map(fileSlice => {
        var isInvalid = false
        try {
          BucketIdentifier.bucketIdFromFileId(fileSlice.getFileId)
        } catch {
          case _: Exception => isInvalid = true
        }
        (isInvalid, fileSlice.getPartitionPath, fileSlice.getFileId)
      })
      .filter(u => u._1)
      .map(u => Row(u._2, u._3))
      .collect()
  }

  override def build: Procedure = new CheckBucketFileIdProcedure()
}

object CheckBucketFileIdProcedure {
  val NAME = "check_bucket_fileid"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new CheckBucketFileIdProcedure
  }
}
