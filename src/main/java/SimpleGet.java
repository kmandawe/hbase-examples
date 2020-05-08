import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class SimpleGet {

  private static byte[] PERSONAL_CF = toBytes("personal");
  private static byte[] PROFESSIONAL_CF = toBytes("professional");

  private static byte[] NAME_COLUMN = toBytes("name");
  private static byte[] FIELD_COLUMN = toBytes("field");

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("census"))) {

      Get get = new Get(toBytes("1"));
      get.addColumn(PERSONAL_CF, NAME_COLUMN);
      get.addColumn(PROFESSIONAL_CF, FIELD_COLUMN);

      Result result = table.get(get);

      byte[] nameValue = result.getValue(PERSONAL_CF, NAME_COLUMN);
      System.out.println("Name: " + Bytes.toString(nameValue));

      byte[] fieldValue = result.getValue(PROFESSIONAL_CF, FIELD_COLUMN);
      System.out.println("Field: " + Bytes.toString(fieldValue));

      System.out.println();
      System.out.println("SimpleGet multiple results in one go:");

      List<Get> getList = new ArrayList<>();

      Get get1 = new Get(toBytes("2"));
      get1.addColumn(PERSONAL_CF, NAME_COLUMN);

      Get get2 = new Get(toBytes("3"));
      get1.addColumn(PERSONAL_CF, NAME_COLUMN);

      getList.add(get1);
      getList.add(get2);

      Result[] results = table.get(getList);
      for (Result res: results) {
        nameValue = res.getValue(PERSONAL_CF, NAME_COLUMN);
        System.out.println("Name: " + Bytes.toString(nameValue));
      }

    }
  }
}
