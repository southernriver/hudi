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

class TestCheckBucketFileIdProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call check_bucket_fileid Procedure by Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           | id int,
           | name string,
           | price double,
           | ts long
           | ) using hudi
           |  partitioned by (ts)
           | location '${tmp.getCanonicalPath}'
           | tblproperties (
           |   primaryKey = 'id',
           |   type = 'cow',
           |   preCombineField = 'ts'
           | )
           |""".stripMargin)

      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

      val result1 = spark.sql(s"call check_bucket_fileid(table => '$tableName')")
        .collect()
      assertResult(1)(result1.length)
      assertResult("ts=1000")(result1(0)(0))
    }
  }
}
