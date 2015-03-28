package bean;

import org.apache.cassandra.utils.UUIDGen;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 旭东 on 2015/3/28.
 */
public class CassandraRow {

    public  ArrayList buildRow(List<String> line, ArrayList columnDefList) throws ParseException {

        ArrayList row = new ArrayList();

        SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

        for (int i = 0; i < line.size(); i++) {

            String columnValue=line.get(i);

            String s = columnDefList.get(i+1).toString();
            if (s.equals("ascii")) {
                row.add(columnValue);

            } else if (s.equals("bigint")) {
                row.add(Long.parseLong(columnValue));

            } else if (s.equals("decimal")) {
                row.add(new BigDecimal(columnValue));

            } else if (s.equals("int")) {
                row.add(new Integer(columnValue));

            } else if (s.equals("text")) {
                row.add(columnValue);

            } else if (s.equals("timestamp")) {
                row.add(DATE_FORMAT.parse(columnValue));

            } else if (s.equals("timeuuid")) {
                row.add(UUIDGen.getTimeUUID());

            } else {
                row.add(new String("null"));

            }

        }

        return row;
    }
}
