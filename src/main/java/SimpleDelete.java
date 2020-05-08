import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;

public class SimpleDelete {
  private static byte[] PERSONAL_CF = toBytes("personal");
  private static byte[] PROFESSIONAL_CF = toBytes("professional");

  private static byte[] GENDER_COLUMN = toBytes("gender");
  private static byte[] FIELD_COLUMN = toBytes("field");

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("census"))) {
      Delete delete = new Delete(toBytes("1"));
      delete.addColumn(PERSONAL_CF, GENDER_COLUMN);
      delete.addColumn(PROFESSIONAL_CF, FIELD_COLUMN);

      table.delete(delete);
      System.out.println("Delete complete!");
    }
  }
}
