package me.ario.cs;

import me.ario.ex.InvalidControlFileFormat;
import org.json.simple.JSONObject;
import org.omg.CORBA.DynAnyPackage.Invalid;
import org.omg.CORBA.INVALID_ACTIVITY;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Created by 旭东 on 2015/3/28.
 */
public class Controller {

    private static final String KEY_SPACE = "keySpace";
    private static final String COLUMN_FAMILY = "columnFamily";
    private static final String COLUMNS = "columns";
    private static final String COLUMNS_DEF = "columnsDef";

    private String controllerName;
    private String[] columnList;
    private String[] columnDefList;
    private String keySpace;
    private String columnFamily;
    private LinkedHashMap controlParam;

    public Controller(String fileName) throws IOException {
        this.controlParam = readControlFile(fileName);
    }

    /**
     * @return return controller name
     */
    public String getControllerName() {
        return controllerName;
    }

    /**
     * @return return all the conlumns name
     */
    public Object getColumnList() {
        return (String[]) controlParam.get(COLUMNS);
    }

    /**
     * @return return all the conlumns' defination
     */
    public Object[] getColumnDefList() {
        return  controlParam.values().toArray();

    }
    /**
     * @return return the keyspace name
     */
    public String getKeySpace() {
        return (String) controlParam.get(KEY_SPACE);
    }

    /**
     * @return return the columns family name
     */
    public String getColumnFamily() {
        return (String) controlParam.get(COLUMN_FAMILY);
    }


    /**
     * @return insert statement
     * <p/>
     * insert keyspace.columnfamily(value1,value2...valueN) values(?,?,....?)
     */
    public String buildInsertSTMT() {

        Object[] columns = controlParam.keySet().toArray();
        String keySpace = (String) controlParam.get(KEY_SPACE);
        String columnFamily = (String) controlParam.get(COLUMN_FAMILY);
        String InsertStatement = "INSERT INTO %s.%s (%s)values(%s)";
        String InsertColumns = "";
        String InsertMapKey = "";

        for (int i = 2; i <= columns.length - 2; i++) {
            InsertColumns = InsertColumns.concat((String) columns[i]).concat(",");
            InsertMapKey = InsertMapKey.concat("?,");
        }
        InsertColumns=InsertColumns.concat((String) columns[columns.length-1]);
        InsertMapKey=InsertMapKey.concat("?");

        return String.format(InsertStatement, keySpace, columnFamily,InsertColumns,InsertMapKey);
    }

    /**
     * @param fileName control file
     * @return {"keySpace" :"",
     * "columnFamily":"",
     * "columns":[],
     * "columnsDef":[]
     * }
     */
    private LinkedHashMap readControlFile(String fileName) throws IOException {
        File file = new File(fileName);
        BufferedReader reader = null;

        LinkedHashMap controlParam = new LinkedHashMap();
        ArrayList columnNameList = new ArrayList();
        ArrayList columnDefList = new ArrayList();
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                if ("".equalsIgnoreCase(tempString)) {
                    continue;
                }
                String[] kv = tempString.trim().split("\\s+");
                String columnName;
                String columnDef;
                if (line == 1) {
                    controlParam.put(KEY_SPACE, kv[0].trim());
                    controlParam.put(COLUMN_FAMILY, kv[1].split(",")[0].trim());
                } else {
                    //todo check timestamp format
                    columnName=kv[0].trim();
                    if (kv.length == 2) {
                        columnDef=kv[1].split(",")[0].trim();
                    } else if (kv.length == 3) {
                        columnDef=kv[1].trim() + " " + kv[2].split(",")[0];
                    } else {
                        throw new InvalidControlFileFormat(line);
                    }
                    controlParam.put(columnName,columnDef);
                }
                line++;
            }
            reader.close();
        } catch (IOException e) {
            throw new IOException(String.format("Unexpected error happens when read file", e.getMessage()));
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new IOException(String.format("Unexpected error happens when read file", e.getMessage()));
                }
            }
        }
        return controlParam;
    }
}
