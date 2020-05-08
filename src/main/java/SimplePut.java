import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class SimplePut {

  private static byte[] PERSONAL_CF = toBytes("personal");
  private static byte[] PROFESSIONAL_CF = toBytes("professional");

  private static byte[] NAME_COLUMN = toBytes("name");
  private static byte[] GENDER_COLUMN = toBytes("gender");
  private static byte[] MARITAL_STATUS_COLUMN = toBytes("marital_status");

  private static byte[] EMPLOYED_COLUMN = toBytes("employed");
  private static byte[] FIELD_COLUMN = toBytes("field");

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("census"))) {

      Put put1 = new Put(toBytes("1"));

      put1.addColumn(PERSONAL_CF, NAME_COLUMN, toBytes("Mike Jones"));
      put1.addColumn(PERSONAL_CF, GENDER_COLUMN, toBytes("male"));
      put1.addColumn(PERSONAL_CF, MARITAL_STATUS_COLUMN, toBytes("unmarried"));

      put1.addColumn(PROFESSIONAL_CF, EMPLOYED_COLUMN, toBytes("yes"));
      put1.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, toBytes("construction"));

      table.put(put1);

      System.out.println("Inserted row for Mike Jones");

      Put put2 = new Put(toBytes("2"));

      put2.addColumn(PERSONAL_CF, NAME_COLUMN, toBytes("Hillary Clinton"));
      put2.addColumn(PERSONAL_CF, GENDER_COLUMN, toBytes("female"));
      put2.addColumn(PERSONAL_CF, MARITAL_STATUS_COLUMN, toBytes("married"));

      put2.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, toBytes("politics"));

      Put put3 = new Put(toBytes("3"));

      put3.addColumn(PERSONAL_CF, NAME_COLUMN, toBytes("Donald Trump"));
      put3.addColumn(PERSONAL_CF, GENDER_COLUMN, toBytes("male"));

      put3.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, toBytes("politics, real estate"));

      List<Put> putList = new ArrayList<>();
      putList.add(put2);
      putList.add(put3);

      table.put(putList);
      System.out.println("Inserted rows for Hillary Clinton and Donald Trump");
    }
  }
}
