/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hudi.HoodieSparkUtils.toAttribute
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.SPARK_VERSION
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.execution.SQLConfInjectingRDD
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

private[hudi] trait SparkVersionsSupport {
  def getSparkVersion: String

  def isSpark2: Boolean = getSparkVersion.startsWith("2.")
  def isSpark3: Boolean = getSparkVersion.startsWith("3.")
  def isSpark3_0: Boolean = getSparkVersion.startsWith("3.0")
  def isSpark3_1: Boolean = getSparkVersion.startsWith("3.1")
  def isSpark3_2: Boolean = getSparkVersion.startsWith("3.2")
  def isSpark3_3: Boolean = getSparkVersion.startsWith("3.3")

  def gteqSpark3_0: Boolean = getSparkVersion >= "3.0"
  def gteqSpark3_1: Boolean = getSparkVersion >= "3.1"
  def gteqSpark3_1_3: Boolean = getSparkVersion >= "3.1.3"
  def gteqSpark3_2: Boolean = getSparkVersion >= "3.2"
  def gteqSpark3_2_1: Boolean = getSparkVersion >= "3.2.1"
  def gteqSpark3_3: Boolean = getSparkVersion >= "3.3"
}

object HoodieSparkUtils extends SparkAdapterSupport with SparkVersionsSupport {

  override def getSparkVersion: String = SPARK_VERSION

  def getMetaSchema: StructType = {
    StructType(HoodieRecord.HOODIE_META_COLUMNS.asScala.map(col => {
      StructField(col, StringType, nullable = true)
    }))
  }

  /**
   * Convert Filters to Catalyst Expressions and joined by And. If convert success return an
   * Non-Empty Option[Expression],or else return None.
   */
  def convertToCatalystExpressions(filters: Seq[Filter],
                                   tableSchema: StructType): Seq[Option[Expression]] = {
    filters.map(convertToCatalystExpression(_, tableSchema))
  }


  /**
   * Convert Filters to Catalyst Expressions and joined by And. If convert success return an
   * Non-Empty Option[Expression],or else return None.
   */
  def convertToCatalystExpression(filters: Array[Filter],
                                  tableSchema: StructType): Option[Expression] = {
    val expressions = convertToCatalystExpressions(filters, tableSchema)
    if (expressions.forall(p => p.isDefined)) {
      if (expressions.isEmpty) {
        None
      } else if (expressions.length == 1) {
        expressions.head
      } else {
        Some(expressions.map(_.get).reduce(org.apache.spark.sql.catalyst.expressions.And))
      }
    } else {
      None
    }
  }

  /**
   * Convert Filter to Catalyst Expression. If convert success return an Non-Empty
   * Option[Expression],or else return None.
   */
  def convertToCatalystExpression(filter: Filter, tableSchema: StructType): Option[Expression] = {
    Option(
      filter match {
        case EqualTo(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.EqualTo(toAttribute(attribute, tableSchema), Literal.create(value))
        case EqualNullSafe(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.EqualNullSafe(toAttribute(attribute, tableSchema), Literal.create(value))
        case GreaterThan(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.GreaterThan(toAttribute(attribute, tableSchema), Literal.create(value))
        case GreaterThanOrEqual(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual(toAttribute(attribute, tableSchema), Literal.create(value))
        case LessThan(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.LessThan(toAttribute(attribute, tableSchema), Literal.create(value))
        case LessThanOrEqual(attribute, value) =>
          org.apache.spark.sql.catalyst.expressions.LessThanOrEqual(toAttribute(attribute, tableSchema), Literal.create(value))
        case In(attribute, values) =>
          val attrExp = toAttribute(attribute, tableSchema)
          val valuesExp = values.map(v => Literal.create(v))
          org.apache.spark.sql.catalyst.expressions.In(attrExp, valuesExp)
        case IsNull(attribute) =>
          org.apache.spark.sql.catalyst.expressions.IsNull(toAttribute(attribute, tableSchema))
        case IsNotNull(attribute) =>
          org.apache.spark.sql.catalyst.expressions.IsNotNull(toAttribute(attribute, tableSchema))
        case And(left, right) =>
          val leftExp = convertToCatalystExpression(left, tableSchema)
          val rightExp = convertToCatalystExpression(right, tableSchema)
          if (leftExp.isEmpty || rightExp.isEmpty) {
            null
          } else {
            org.apache.spark.sql.catalyst.expressions.And(leftExp.get, rightExp.get)
          }
        case Or(left, right) =>
          val leftExp = convertToCatalystExpression(left, tableSchema)
          val rightExp = convertToCatalystExpression(right, tableSchema)
          if (leftExp.isEmpty || rightExp.isEmpty) {
            null
          } else {
            org.apache.spark.sql.catalyst.expressions.Or(leftExp.get, rightExp.get)
          }
        case Not(child) =>
          val childExp = convertToCatalystExpression(child, tableSchema)
          if (childExp.isEmpty) {
            null
          } else {
            org.apache.spark.sql.catalyst.expressions.Not(childExp.get)
          }
        case StringStartsWith(attribute, value) =>
          val leftExp = toAttribute(attribute, tableSchema)
          val rightExp = Literal.create(s"$value%")
          sparkAdapter.getCatalystPlanUtils.createLike(leftExp, rightExp)
        case StringEndsWith(attribute, value) =>
          val leftExp = toAttribute(attribute, tableSchema)
          val rightExp = Literal.create(s"%$value")
          sparkAdapter.getCatalystPlanUtils.createLike(leftExp, rightExp)
        case StringContains(attribute, value) =>
          val leftExp = toAttribute(attribute, tableSchema)
          val rightExp = Literal.create(s"%$value%")
          sparkAdapter.getCatalystPlanUtils.createLike(leftExp, rightExp)
        case _ => null
      }
    )
  }

  /**
   * @deprecated please use other overload [[createRdd]]
   */
  def createRdd(df: DataFrame, structName: String, recordNamespace: String, reconcileToLatestSchema: Boolean,
                latestTableSchema: org.apache.hudi.common.util.Option[Schema] = org.apache.hudi.common.util.Option.empty()): RDD[GenericRecord] = {
    var latestTableSchemaConverted : Option[Schema] = None

    if (latestTableSchema.isPresent && reconcileToLatestSchema) {
      latestTableSchemaConverted = Some(latestTableSchema.get())
    } else {
      // cases when users want to use latestTableSchema but have not turned on reconcileToLatestSchema explicitly
      // for example, when using a Transformer implementation to transform source RDD to target RDD
      latestTableSchemaConverted = if (latestTableSchema.isPresent) Some(latestTableSchema.get()) else None
    }
    createRdd(df, structName, recordNamespace, latestTableSchemaConverted)
  }

  private def toAttribute(columnName: String, tableSchema: StructType): AttributeReference = {
    val field = tableSchema.find(p => p.name == columnName)
    assert(field.isDefined, s"Cannot find column: $columnName, Table Columns are: " +
      s"${tableSchema.fieldNames.mkString(",")}")
    AttributeReference(columnName, field.get.dataType, field.get.nullable)()
  }

  def createRdd(df: DataFrame, structName: String, recordNamespace: String, readerAvroSchemaOpt: Option[Schema]): RDD[GenericRecord] = {
    val writerSchema = df.schema
    val writerAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(writerSchema, structName, recordNamespace)
    val readerAvroSchema = readerAvroSchemaOpt.getOrElse(writerAvroSchema)
    // We check whether passed in reader schema is identical to writer schema to avoid costly serde loop of
    // making Spark deserialize its internal representation [[InternalRow]] into [[Row]] for subsequent conversion
    // (and back)
    val sameSchema = writerAvroSchema.equals(readerAvroSchema)
    val (nullable, _) = AvroConversionUtils.resolveAvroTypeNullability(writerAvroSchema)

    // NOTE: We have to serialize Avro schema, and then subsequently parse it on the executor node, since Spark
    //       serializer is not able to digest it
    val readerAvroSchemaStr = readerAvroSchema.toString
    val writerAvroSchemaStr = writerAvroSchema.toString

    // NOTE: We're accessing toRdd here directly to avoid [[InternalRow]] to [[Row]] conversion
    //       Additionally, we have to explicitly wrap around resulting [[RDD]] into the one
    //       injecting [[SQLConf]], which by default isn't propgated by Spark to the executor(s).
    //       [[SQLConf]] is required by [[AvroSerializer]]
    injectSQLConf(df.queryExecution.toRdd.mapPartitions { rows =>
      if (rows.isEmpty) {
        Iterator.empty
      } else {
        val readerAvroSchema = new Schema.Parser().parse(readerAvroSchemaStr)
        val transform: GenericRecord => GenericRecord =
          if (sameSchema) identity
          else {
            HoodieAvroUtils.rewriteRecordDeep(_, readerAvroSchema)
          }

        // Since caller might request to get records in a different ("evolved") schema, we will be rewriting from
        // existing Writer's schema into Reader's (avro) schema
        val writerAvroSchema = new Schema.Parser().parse(writerAvroSchemaStr)
        val convert = AvroConversionUtils.createInternalRowToAvroConverter(writerSchema, writerAvroSchema, nullable = nullable)

        rows.map { ir => transform(convert(ir)) }
      }
    }, SQLConf.get)
  }

  def getCatalystRowSerDe(structType: StructType) : SparkRowSerDe = {
    sparkAdapter.createSparkRowSerDe(structType)
  }

  private def injectSQLConf[T: ClassTag](rdd: RDD[T], conf: SQLConf): RDD[T] =
    new SQLConfInjectingRDD(rdd, conf)
}
