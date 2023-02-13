/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.procedure

class TestDropPartitionsProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call drop_partitions Procedure With single-partition Pruning") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  preCombineField = 'ts'
             | )
             | partitioned by(ts)
             | location '$basePath'
       """.stripMargin)

        // Test partition pruning with single predicate
        var resultA: Array[Seq[Any]] = Array.empty

        {
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

          checkException(
            s"call drop_partitions(table => '$tableName', predicate => 'ts <= 1001L and id = 10')"
          )("Only partition predicates are allowed")

          // Do table drop partitions with partition predicate
          resultA = spark.sql(s"call drop_partitions(table => '$tableName', predicate => 'ts <= 1001L')")
            .collect()
            .map(row => Seq(row.getBoolean(0), row.getString(1)))
          assertResult(1)(resultA.length)

          assertResult(1) {
            spark.sql(s"show partitions $tableName").collect().length
          }
        }
      }
    }
  }
}
