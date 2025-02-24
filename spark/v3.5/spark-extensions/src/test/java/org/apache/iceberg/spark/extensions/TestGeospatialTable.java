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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.geo.testing.GeographyUDT$;
import org.apache.iceberg.spark.geo.testing.GeometryUDT$;
import org.apache.iceberg.spark.geo.testing.TestGeography;
import org.apache.iceberg.spark.geo.testing.TestingGeospatialLibraryInitializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

@ExtendWith(ParameterizedTestExtension.class)
public class TestGeospatialTable extends ExtensionsTestBase {

  private static final GeometryFactory FACTORY = new GeometryFactory();

  @BeforeAll
  public static void registerSpatial() {
    TestingGeospatialLibraryInitializer.initialize(spark);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testCreateGeometryTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, part INT, geom GEOMETRY) USING iceberg PARTITIONED BY (part) TBLPROPERTIES ('format-version' = '3')",
        tableName);

    Object createTableStmt = scalarSql("SHOW CREATE TABLE %s", tableName);
    assertThat(createTableStmt.toString().contains("USING iceberg")).isTrue();
    List<Object[]> tableDesc = sql("DESCRIBE TABLE EXTENDED %s", tableName);
    tableDesc.stream()
        .filter(row -> row[0].equals("Provider"))
        .forEach(row -> assertThat(row[1]).isEqualTo("iceberg"));

    // write some data
    Dataset<Row> df = createTestGeometryDf();
    df.write().format("iceberg").mode("append").save(tableName);
    List<Object[]> rows = sql("SELECT * FROM %s", tableName);
    assertThat(rows.size()).isEqualTo(40);
    rows.forEach(
        row -> {
          assertThat(row.length).isEqualTo(3);
          assertThat(row[2]).isInstanceOf(Geometry.class);
        });

    // query some data using spatial predicate
    String testSql =
        String.format(
            "SELECT * FROM %s WHERE ST_Covers(ST_PolygonFromEnvelope(0, 0, 5, 5), geom)",
            tableName);
    Pattern executedPlanPattern = Pattern.compile(".*BatchScan.*st_intersects\\(geom.*");
    String executedPlan = spark.sql(testSql).queryExecution().executedPlan().toString();
    assertThat(executedPlanPattern.matcher(executedPlan).find()).isTrue();
    rows = sql(testSql);
    assertThat(rows.size()).isEqualTo(5);
    rows.forEach(row -> assertThat((Integer) row[0] < 5).isTrue());

    // query using negated spatial predicate
    testSql =
        String.format(
            "SELECT * FROM %s WHERE NOT ST_Intersects(ST_PolygonFromEnvelope(0, 0, 5, 5), geom)",
            tableName);
    rows = sql(testSql);
    assertThat(rows.size()).isEqualTo(35);
  }

  @TestTemplate
  public void testUpdateGeospatialTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, part INT, geom GEOMETRY) USING iceberg PARTITIONED BY (part) TBLPROPERTIES ('format-version' = '3')",
        tableName);
    Dataset<Row> df = createTestGeometryDf();
    df.write().format("iceberg").mode("append").save(tableName);
    sql(
        "UPDATE %s SET geom = ST_Buffer(geom, 0.5) WHERE ST_Intersects(geom, ST_PolygonFromEnvelope(0, 0, 4.5, 4.5))",
        tableName);
    List<Object[]> rows = sql("SELECT * FROM %s", tableName);
    assertThat(rows).hasSize(40);
    rows.forEach(
        row -> {
          int id = (Integer) row[0];
          Geometry geom = (Geometry) row[2];
          if (id < 4) {
            assertThat(geom instanceof Polygon).isTrue();
          } else {
            assertThat(geom instanceof Point).isTrue();
          }
        });
  }

  @TestTemplate
  public void testGeographyTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, part INT, geog GEOGRAPHY) USING iceberg PARTITIONED BY (part) TBLPROPERTIES ('format-version' = '3')",
        tableName);

    Object createTableStmt = scalarSql("SHOW CREATE TABLE %s", tableName);
    assertThat(createTableStmt.toString().contains("USING iceberg")).isTrue();
    List<Object[]> tableDesc = sql("DESCRIBE TABLE EXTENDED %s", tableName);
    tableDesc.stream()
        .filter(row -> row[0].equals("Provider"))
        .forEach(row -> assertThat(row[1]).isEqualTo("iceberg"));

    // write some data
    Dataset<Row> df = createTestGeographyDf();
    df.write().format("iceberg").mode("append").save(tableName);
    List<Object[]> rows = sql("SELECT * FROM %s", tableName);
    assertThat(rows.size()).isEqualTo(40);
    rows.forEach(
        row -> {
          assertThat(row.length).isEqualTo(3);
          assertThat(row[2]).isInstanceOf(TestGeography.class);
          int id = (int) row[0];
          int part = (int) row[1];
          TestGeography geog = (TestGeography) row[2];
          Coordinate coordinate = geog.geometry().getCoordinate();
          assertThat(coordinate.x).isEqualTo(id % 10);
          assertThat(coordinate.y).isEqualTo(id % 10 + part);
        });
  }

  private Dataset<Row> createTestGeometryDf() {
    List<Row> rows = Lists.newArrayList();
    int id = 0;
    for (int i = 0; i < 4; i++) {
      for (int k = 1; k <= 10; k++) {
        Coordinate coordinate = null;
        switch (i) {
          case 0:
            coordinate = new Coordinate(k, k);
            break;
          case 1:
            coordinate = new Coordinate(-k, k);
            break;
          case 2:
            coordinate = new Coordinate(-k, -k);
            break;
          case 3:
            coordinate = new Coordinate(k, -k);
            break;
        }
        Row row = new GenericRow(new Object[] {id++, i, FACTORY.createPoint(coordinate)});
        rows.add(row);
      }
    }
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("part", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("geom", GeometryUDT$.MODULE$, false, Metadata.empty())
            });
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> createTestGeographyDf() {
    List<Row> rows = Lists.newArrayList();
    int id = 0;
    for (int i = 0; i < 4; i++) {
      for (int k = 0; k < 10; k++) {
        Coordinate coordinate = new Coordinate(k, k + i);
        TestGeography geography = TestGeography.apply(FACTORY.createPoint(coordinate));
        Row row = new GenericRow(new Object[] {id++, i, geography});
        rows.add(row);
      }
    }
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("part", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("geog", GeographyUDT$.MODULE$, false, Metadata.empty())
            });
    return spark.createDataFrame(rows, schema);
  }
}
