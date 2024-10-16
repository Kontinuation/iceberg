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
package org.apache.iceberg.parquet.havasu;

import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.havasu.GeometryFieldMetrics;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.types.Types.GeometryType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;

public class ParquetGeometryValueWriters {

  private ParquetGeometryValueWriters() {}

  public static ParquetValueWriters.PrimitiveWriter<Geometry> buildWriter(
      GeometryType geometryType, ColumnDescriptor desc) {
    return new GeometryWKBWriter(desc);
  }

  private static class GeometryWKBWriter
      extends ParquetValueWriters.PrimitiveWriter<Geometry> {

    private final GeometryFieldMetrics.GenericBuilder metricsBuilder;

    GeometryWKBWriter(ColumnDescriptor desc) {
      super(desc);
      int id = desc.getPrimitiveType().getId().intValue();
      metricsBuilder = new GeometryFieldMetrics.GenericBuilder(id);
    }

    @Override
    public void write(int rl, Geometry geom) {
      WKBWriter wkbWriter = new WKBWriter(getDimension(geom), false);
      byte[] wkb = wkbWriter.write(geom);
      column.writeBinary(rl, Binary.fromReusedByteArray(wkb));
      metricsBuilder.add(geom);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(metricsBuilder.build());
    }
  }

  private static int getDimension(Geometry geom) {
    return geom.getCoordinate() != null && !java.lang.Double.isNaN(geom.getCoordinate().getZ())
        ? 3
        : 2;
  }
}
