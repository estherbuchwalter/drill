package org.apache.drill.exec.fn.impl;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDateTypeAutoFunctions extends ClusterTest {
  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testDateAddAuto() throws RpcException {
    String sql = "select date_add_auto('2015/01/24', '3') as col1 " +
      "from (values(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder().add("col1", TypeProtos.MinorType.TIMESTAMP).build();

    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(2015-01-27).build();

    new RowSetComparison(expected).verifyAndClearAll(results);
}
/*
  @Test
  public void testDateAddAuto() throws Exception {
    String query = "select date_add('2015-01-24', '3') as col1 " +
      "from (values(1))";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues("2015-01-27")
      .go();
  }

 */

}
