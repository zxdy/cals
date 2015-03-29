#Cals

Cals is a tool to create SSTable from csv , and then you can use sstableloader to load big data to Cassandra database.

#Features

* Use schema and control file to convert csv file
* Support `timeuuid`,`bigint`,`decimal`,`int`,`text`,`timestamp` column type
* Add default value to null column
    * bigint -> 0
    * decimal -> 0
    * int -> 0
    * text -> "null"
    * timestamp -> "1970-01-01"

**note:**

1. To use this tool, your table's first column must defined as  timeuuid
2. The csv file should have a header
3. The scheama and control file's format is very important,you need create them exactly the same foramt as the samples.

#How TO Use

```
usage: java -jar cals.jar <schemaFile> <controlFile> <dataFile> <outPutFolder>
```

## 1. Create Schema file

Basically , it's the same as the DDL statement when you create the table(column family). just keep in mind **DO NOT** lost the keyspace in the statement.

**smaple:**
```
CREATE TABLE IF NOT EXISTS quote.historical_prices (
    id timeuuid,
    date timestamp,
    open decimal,
    high decimal,
    low decimal,
    close decimal,
    volume bigint,
    adj_close decimal,
    lucene text,
    PRIMARY KEY (id, date)
);
```

## 2. Create Control file

If you have ever used the oracle's sqlldr before, you will be much understand of it. This is the key component to map the csv fields to table columns.Control file's name is not that important for this tool , but I still sugguest you name it as the table's name. The below tips are critical

*  **Fisrt Line** is used to identify the keysapce and table name. format:
`<keyspace> <tablename>,`

*  **Following Lines** are used to identify the columns you are going to load data.format:`<columnname> <defination>,`

    1.  The column name and it's defination must be the same as schema file.
    2.  The orders must be the same as the data in csv file.
    3.  Case sensitive.please use lower case in both control file and schema file
    4.  each line must end with `,`

**sample:**
```
quote historical_prices,
id timeuuid,
date timestamp,
open decimal,
high decimal,
low decimal,
close decimal,
volume bigint,
adj_close decimal,
```

## 3. Prepare the CSV file

* Add a header for each csv file, or you will lost one line's data

**sample:**
```
Date,Open,High,Low,Close,Volume,Adj Close
2015-03-25,570.50,572.26,558.74,558.78,2110700,558.78
2015-03-24,562.56,574.59,561.21,570.19,2570100,570.19
2015-03-23,560.43,562.36,555.83,558.81,1625600,558.81
2015-03-20,561.65,561.72,559.05,560.36,2585800,560.36
```
