package me.ario.cs;

import it.unimi.dsi.fastutil.Stack;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * DDL class
 *
 */
public class Schema {

    private static String schemaContext;

    /**
     * @param fileName
     * @throws IOException
     */
    public Schema(String fileName) throws IOException {
        String fileContext = readFileByLines(fileName);
        this.schemaContext = fileContext;
    }

    /**
     * @return return the ddl string
     */
    public String getSchemaContext() {
        return schemaContext;
    }

    /**
     * @param fileName config file
     * @return file context
     */
    private String readFileByLines(String fileName) throws IOException {
        File file = new File(fileName);
        BufferedReader reader = null;
        String fileContext = "";
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                fileContext = fileContext + tempString;
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
                    throw new IOException(String.format("Unexpected error happens when close file", e.getMessage()));
                }
            }
        }
        return fileContext;
    }
}
