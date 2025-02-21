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
package org.apache.iceberg.spark.geo.spi;

import org.apache.iceberg.Geography;
import org.apache.iceberg.expressions.Expression;
import org.apache.spark.sql.types.DataType;
import org.locationtech.jts.geom.Geometry;

public interface GeospatialLibrary {
  DataType getGeometryType();

  DataType getGeographyType();

  Object fromGeometry(Geometry geometry);

  Geometry toGeometry(Object geometry);

  Object fromGeography(Geography geography);

  Geography toGeography(Object geography);

  boolean isSpatialFilter(org.apache.spark.sql.catalyst.expressions.Expression sparkExpression);

  Expression translateToIceberg(
      org.apache.spark.sql.catalyst.expressions.Expression sparkExpression);
}
