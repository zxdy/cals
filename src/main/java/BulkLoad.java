/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Usage: java bulkload.BulkLoad
 */
public class BulkLoad {
    /**
     * Default output directory
     */
    public static final String DEFAULT_OUTPUT_DIR = "D:\\workspace\\cals\\src\\main\\resources\\";

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Keyspace name
     */
    public static final String KEYSPACE = "quote";
    /**
     * Table name
     */
    public static final String TABLE = "historical_prices";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String SCHEMA = String.format("CREATE TABLE %s.%s (" +
            "date timestamp, " +
            "open decimal, " +
            "high decimal, " +
            "low decimal, " +
            "close decimal, " +
            "volume bigint, " +
            "adj_close decimal, " +
            "PRIMARY KEY (ticker, date) " +
            ") WITH CLUSTERING ORDER BY (date DESC)", KEYSPACE, TABLE);

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (" +
            "ticker, date, open, high, low, close, volume, adj_close" +
            ") VALUES (" +
            "?, ?, ?, ?, ?, ?, ?, ?" +
            ")", KEYSPACE, TABLE);

    public static void main(String[] args) throws IOException {
        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        // Prepare SSTable writer
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
        String fileName="D:\\workspace\\cals\\src\\main\\resources\\table.csv";
        BufferedReader reader = null;
        CsvListReader csvReader = null;
        try {
                reader = new BufferedReader(new InputStreamReader((new FileInputStream(fileName))));
                csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE);

                csvReader.getHeader(true);

                // Write to SSTable while reading data
                List<String> line;
                while ((line = csvReader.read()) != null) {
                    // We use Java types here based on
                    // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
                    writer.addRow(
                            DATE_FORMAT.parse(line.get(0)),
                            new BigDecimal(line.get(1)),
                            new BigDecimal(line.get(2)),
                            new BigDecimal(line.get(3)),
                            new BigDecimal(line.get(4)),
                            Long.parseLong(line.get(5)),
                            new BigDecimal(line.get(6)));
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (InvalidRequestException e){
                e.printStackTrace();
            } finally {
            csvReader.close();
            reader.close();
            writer.close();
        }
        System.exit(0);
    }
}
