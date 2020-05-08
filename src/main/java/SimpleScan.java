import java.io.IOException;
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

public class SimpleScan {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    Scan scan = new Scan();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("census"));
        ResultScanner scanResult = table.getScanner(scan)) {
      for (Result res: scanResult) {
        for (Cell cell: res.listCells()) {
          String row = new String(CellUtil.cloneRow(cell));
          String family = new String(CellUtil.cloneFamily(cell));
          String column = new String(CellUtil.cloneQualifier(cell));
          String value = new String(CellUtil.cloneValue(cell));
          System.out.println(row + " " + family + " " + column + " " + value);
        }
      }
    }
  }
}
