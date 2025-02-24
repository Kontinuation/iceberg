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
package org.apache.iceberg.spark.geo.testing

import org.apache.iceberg.util.GeometryUtil
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.UserDefinedType

/** A Geography UDT for testing the geospatial library integration support in Iceberg Spark. */
class GeographyUDT extends UserDefinedType[TestGeography] {

  def sqlType(): DataType = DataTypes.BinaryType

  def serialize(obj: TestGeography): Any = GeometryUtil.toWKB(obj.geometry)

  def deserialize(datum: Any): TestGeography = {
    datum match {
      case bytes: Array[Byte] => TestGeography(GeometryUtil.fromWKB(bytes))
      case _ => throw new IllegalArgumentException(
        s"Expected an Array[Byte] object but got ${datum.getClass.getName}")
    }
  }

  def userClass: Class[TestGeography] = {
    classOf[TestGeography]
  }

  override def equals(other: Any): Boolean = other match {
    case _: UserDefinedType[_] => other.isInstanceOf[GeographyUDT]
    case _ => false
  }

  override def hashCode(): Int = userClass.hashCode()
}

object GeographyUDT extends GeographyUDT
