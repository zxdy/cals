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
        for (int i = 0; i < line.size(); i++) {
            String columnValue=line.get(i);
            if(!"".equals(columnValue) && columnValue!=null){
                columnValue=columnValue.trim();
            }

//            System.out.print(".."+columnValue);
            String s = columnDefList.get(i+1).toString();
            if (s.equals("ascii")) {
                row.add(columnValue);

            } else if (s.equals("bigint")) {
                row.add(Long.parseLong(isNumeric(columnValue)?columnValue.trim():"0"));

            } else if (s.equals("decimal")) {
                row.add(new BigDecimal(isNumeric(columnValue)?columnValue.trim():"0"));

            } else if (s.equals("int")) {
                row.add(new Integer(isNumeric(columnValue)?columnValue.trim():"0"));

            } else if (s.equals("text")) {
                row.add(checkColumnValue(columnValue)?columnValue:"null");

            } else if (s.contains("timestamp")) {
                String timeFormat="yyyy-MM-dd";
                if (s.contains("<")) {
                    if(s.split("<").length==2){
                        timeFormat=s.split("<")[1];
                    }

                }
//        String timeFormat="MM/dd/yyyy";
//        String timeFormat="yyyy-MM-dd hh:mm:ss";
//        String timeFormat="MM/dd/yyyy hh:mm:ss";
//        String timeFormat="yyyy-MM-dd hh:mm:ss.SSS";
                //todo format time as defined in control file
                SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(timeFormat);
                row.add(DATE_FORMAT.parse(checkColumnValue(columnValue)?columnValue:"1970-01-01"));

            } else if (s.equals("timeuuid")) {
                row.add(UUIDGen.getTimeUUID());

            } else if (s.equals("double")) {
                row.add(new Double(isNumeric(columnValue)?columnValue.trim():"0"));

            }else {
                row.add(new String("null"));

            }

        }

        return row;
    }
    private boolean checkColumnValue(String columnValue){
        if("".equals(columnValue)||columnValue==null){
            return false;
        }else {
            return true;
        }
    }

    public static boolean isNumeric(String str) {
        if("".equals(str)||str==null){
            return false;
        }else {
            return str.matches("[\\+-]?[0-9]+((.)[0-9])*[0-9]*");
        }

    }
}
