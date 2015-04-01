package main0;

import bean.CassandraRow;
import bean.Controller;
import bean.Schema;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.UUIDGen;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 旭东 on 2015/3/27.
 */
public class CalsMain {

    /**
     *
     */
    public static void main(String args[]) throws IOException {
        if (args.length < 4) {
            System.out.println("usage: java -jar cals.jar <schemaFile> <controlFile> <dataFile> <outPutFolder>");
            return;
        }
        long startTime=System.currentTimeMillis();
        String schemaFileName = args[0];
        String controlFileName = args[1];
        String dataFile = args[2];
        String outPutFolder = args[3];


        Config.setClientMode(true);

        Schema schema = new Schema(schemaFileName);
        Controller controller = new Controller(controlFileName);
        CassandraRow crow = new CassandraRow();

        String SCHEMA = schema.getSchemaContext();
        System.out.println(SCHEMA);
        String INSERT_STMT = controller.buildInsertSTMT();
        System.out.println(INSERT_STMT);

        File outputDir = new File(outPutFolder + File.separator + controller.getKeySpace() + File.separator + controller.getColumnFamily());
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
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
            reader = new BufferedReader(new InputStreamReader((new FileInputStream(dataFile))));
            csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE);
//            csvReader.getHeader(false);
            // Write to SSTable while reading data
            List<String> line;
            int lineNum=1;
            ArrayList columnDef = controller.getColumnDefList();
            while ((line = csvReader.read()) != null) {
                // We use Java types here based on
                // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
                System.out.println("processing line..." + lineNum);
                if (line.size()+1 == columnDef.size()) {
                    ArrayList row = new ArrayList();
                    row.add(UUIDGen.getTimeUUID());
                    row.addAll(crow.buildRow(line, columnDef));
                    writer.addRow(row);
                }
                lineNum++;

            }
            long endTime=System.currentTimeMillis();
            System.out.println("time used： "+(endTime-startTime)/1000+"s");
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (InvalidRequestException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (NumberFormatException e){
            e.printStackTrace();
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
        System.exit(0);
    }
}
