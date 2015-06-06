package me.ario.calsroad;

import me.ario.cs.CassandraRow;
import me.ario.cs.Controller;
import me.ario.cs.Schema;
import me.ario.ex.NotExistPathException;
import me.ario.ex.TimeParseException;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.utils.UUIDGen;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ario on 2015/3/27.
 */
public class CalsMain {

    /**
     * @param args args[0] schemaFileName the DDL definition of the table which going to be load data
     * @param args args[1] controlFile the control file which is used the structure the fields
     * @param args args[2] dataFile the csv file which is used to be load
     * @param args args[3] outPutFolder the destination of the folder for sstable
     * @param args args[4] csvheader specify if the csv file have the header
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
        if (args.length < 5) {
            System.out.println("usage: java -jar cals.jar <schemaFile> <controlFile> <dataFile> <outPutFolder> <csvheader:hasheader|noheader>");
            return;
        }
        long startTime = System.currentTimeMillis();
        String schemaFileName = args[0];
        String controlFileName = args[1];
        String dataFile = args[2];
        String outPutFolder = args[3];
        String csvHeader = args[4];


        Config.setClientMode(true);

        Schema schema = new Schema(schemaFileName);
        Controller controller = new Controller(controlFileName);
        CassandraRow crow = new CassandraRow();

        //build the ddl string
        String SCHEMA = schema.getSchemaContext();
        System.out.println(String.format("[*] DDL: %s",SCHEMA));
        //build the insert string
        String INSERT_STMT = controller.buildInsertSTMT();
        System.out.println(String.format("[*] Insert statement: %s",INSERT_STMT));

        //setup output folder
        File outputDir = new File(outPutFolder + File.separator + controller.getKeySpace() + File.separator + controller.getColumnFamily());
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new NotExistPathException(outputDir);
        }


        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir)
                // set target schema
                .forTable(SCHEMA)
                        // set CQL statement to put data
                .using(INSERT_STMT)
                        // set partitioner if needed
                        // default is Murmur3Partitioner so set if you use different one.
                .withPartitioner(new Murmur3Partitioner());


        CQLSSTableWriter writer = builder.build();
        BufferedReader reader = null;
        CsvListReader csvReader = null;
        try {
            reader = new BufferedReader(new InputStreamReader((new FileInputStream(dataFile)), "UTF-8"));
            csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE);
            if ("hasheader".equalsIgnoreCase(csvHeader)) {
                csvReader.getHeader(true);
            }
            List<String> line;
            int lineNum = 1;
            Object[] columnDef = controller.getColumnDefList();

            // core
            // map the csv fields with ddl string and insert statement to build the sstable writer per line,
            while ((line = csvReader.read()) != null) {
                System.out.println("[*] processing line..." + lineNum);
                if (line.size() == columnDef.length-3) {
                    ArrayList row = new ArrayList();
                    row.add(UUIDGen.getTimeUUID());
                    row.addAll(crow.buildRow(line, columnDef));
                    writer.addRow(row);
                }
                lineNum++;

            }
        } catch (IOException e) {
            System.out.print(String.format("file %s not found,exiting...",dataFile));
            System.exit(-1);
        } catch (InvalidRequestException e) {
            System.out.print(String.format("error happens when add row,exiting...", dataFile));
            System.exit(-1);
        } catch (TimeParseException e) {
            System.out.print(e.getMessage());
            System.exit(-1);
        } catch (NumberFormatException e) {
            System.out.print(e.getMessage());
            System.exit(-1);
        } catch (SuperCsvException e) {
            System.out.print(e.getMessage());
            System.exit(-1);
        } catch (ClassCastException e){
            System.out.print(String.format("[-] error '%s' happens when build sstable,check you control file and schema file to be match",e.getMessage()));
            System.exit(-1);
        }finally {
            if (csvReader != null) {
                csvReader.close();
            }
            if (reader != null) {
                reader.close();
            }
            writer.close();
        }
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("[*] sstable saved to :%s",outputDir));
        System.out.println(String.format("[*] sstable created successfully\n[*] time used： %d s",(endTime - startTime) / 1000));
        System.exit(0);
    }
}
