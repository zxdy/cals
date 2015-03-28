package bean;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by 旭东 on 2015/3/28.
 */
public class Schema {

    private String schemaContext;

    public Schema(String fileName){
        String fileContext=readFileByLines(fileName);
        this.schemaContext=fileContext;
    }

    public String getSchemaContext() {
        return schemaContext;
    }

    /**
     * @param fileName config file
     * @return file context
     */
    private String readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        String fileContext="";
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                fileContext=fileContext+tempString;
                line++;
            }
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
        return fileContext;
    }
}
