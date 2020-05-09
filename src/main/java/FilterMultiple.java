import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterMultiple {

  private static void printResults(ResultScanner scanResult) {
    System.out.println();
    System.out.println("Results: ");

    for (Result res : scanResult) {
      for (Cell cell : res.listCells()) {
        String row = new String(CellUtil.cloneRow(cell));
        String family = new String(CellUtil.cloneFamily(cell));
        String column = new String(CellUtil.cloneQualifier(cell));
        String value = new String(CellUtil.cloneValue(cell));

        System.out.println(row + " " + family + " " + column + " " + value);
      }
    }
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("census"))) {

      SingleColumnValueFilter maritalStatusFilter =
          new SingleColumnValueFilter(
              Bytes.toBytes("personal"),
              Bytes.toBytes("marital_status"),
              CompareOp.EQUAL,
              new BinaryComparator(Bytes.toBytes("divorced")));
      maritalStatusFilter.setFilterIfMissing(true);

      SingleColumnValueFilter genderFilter =
          new SingleColumnValueFilter(
              Bytes.toBytes("personal"),
              Bytes.toBytes("gender"),
              CompareOp.EQUAL,
              new BinaryComparator(Bytes.toBytes("female")));
      genderFilter.setFilterIfMissing(true);

      List<Filter> filterList = new ArrayList<>();
      filterList.add(maritalStatusFilter);
      filterList.add(genderFilter);

      FilterList filters = new FilterList(filterList);
      Scan userScan = new Scan();
      userScan.setFilter(filters);

      try (ResultScanner scanResult = table.getScanner(userScan)) {
        printResults(scanResult);
      }

    }
  }
}
