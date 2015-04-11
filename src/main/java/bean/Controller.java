package bean;

import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by 旭东 on 2015/3/28.
 */
public class Controller {

    private static final String KEY_SPACE ="keySpace";
    private static final String COLUMN_FAMILY ="columnFamily";
    private static final String COLUMNS="columns";
    private static final String COLUMNS_DEF ="columnsDef";

    private String controllerName;
    private String[] columnList;
    private String[] columnDefList;
    private String keySpace;
    private String columnFamily;
    private JSONObject controlParam;

    public Controller(String fileName){
        this.controlParam=readControlFile(fileName);
    }

    public String getControllerName() {
        return controllerName;
    }

    public Object getColumnList() {
        return (String[]) controlParam.get(COLUMNS);
    }

    public ArrayList getColumnDefList() {
        return (ArrayList) controlParam.get(COLUMNS_DEF);

    }

    public String getKeySpace() {
        return (String) controlParam.get(KEY_SPACE);
    }

    public String getColumnFamily() {
        return  (String) controlParam.get(COLUMN_FAMILY);
    }


    /**
     * @return insert statement
     *
     * insert keyspace.columnfamily(value1,value2...valueN) values(?,?,....?)
     *
     */
    public String buildInsertSTMT() {

        ArrayList columns = (ArrayList) controlParam.get(COLUMNS);
        String keySpace= (String) controlParam.get(KEY_SPACE);
        String columnFamily = (String) controlParam.get(COLUMN_FAMILY);
        String InsertStatement = "INSERT INTO %s.%s (";
        String InsertColumns = "";
        String InsertMapKey = "";
        for (int i = 0; i < columns.size() - 1; i++) {
            InsertColumns = InsertColumns + columns.get(i) + ",";
            InsertMapKey = InsertMapKey + "?,";
        }

        InsertStatement =InsertStatement+ InsertColumns+columns.get(columns.size() - 1) + ") VALUES (" + InsertMapKey + "?)";


        return String.format(InsertStatement, keySpace, columnFamily);
    }

    /**
     * @param fileName control file
     * @return
     * {"keySpace" :"",
     * "columnFamily":"",
     * "columns":[],
     * "columnsDef":[]
     * }
     */
    private JSONObject readControlFile(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;

        JSONObject controlParam= new JSONObject();
        ArrayList columnNameList= new ArrayList();
        ArrayList columnDefList=new ArrayList();
        HashMap keyValue=new HashMap();


        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                String[] kv = tempString.split("\\s+");
                if( line ==1 ){
                    controlParam.put(KEY_SPACE, kv[0].trim());
                    controlParam.put(COLUMN_FAMILY, kv[1].split(",")[0].trim());
                }else {
                    //todo check timestamp format
                    columnNameList.add(kv[0].trim());
                    if (kv.length==2){
                        columnDefList.add(kv[1].split(",")[0].trim());
                    }else if (kv.length==3){
                        columnDefList.add(kv[1].trim()+" "+kv[2].split(",")[0]);
                    }
                }
                line++;
            }
            controlParam.put(COLUMNS,columnNameList);
            controlParam.put(COLUMNS_DEF,columnDefList);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return controlParam;
    }
}
