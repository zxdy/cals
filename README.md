#Cals

Cals is a tool to create SSTable from csv , and then you can use sstableloader to load big data to Cassandra database.

#Features

* Use schema and control file to convert csv file
* Support `timeuuid`,`bigint`,`decimal`,`int`,`text`,`timestamp`,`double` column type
* Support UTF-8
* Add default value to null column
    * bigint -> 0
    * decimal -> 0
    * double -> 0
    * int -> 0
    * text -> "null"
    * timestamp -> "1970-01-01"

**note:**

1. To use this tool, your table's first column must defined as  timeuuid
2. The csv file should have a header
3. The schema and control file's formats are very important,you need create them exactly the same as the sample's.
4. the timestamp's format is default to "yyyy-MM-dd".

#How TO Use

```
usage: java -jar cals.jar <schemaFile> <controlFile> <dataFile> <outPutFolder> <csvheader:hasheader|noheader>
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

    *  The column name and it's defination must be the same as schema file.
    *  The orders must be the same as the data in csv file.
    *  Case sensitive.please use lower case in both control file and schema file
    *  each line must end with `,`

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

you can also configure the timestamp's format in the control file to match the format of your csv data
below are some samples,**DO NOT** leave blanks around `<`

```
date timestamp, //default as yyyy-MM-dd
date timestamp<yyyy-MM-dd hh:mm:ss,
date timestamp<yyyy-MM-dd hh:mm:ss.SSS,
date timestamp<MM/dd/yyyy hh:mm:ss,
date timestamp<MM/dd/yyyy hh:mm:ss.SSS,
```



## 3. Prepare the CSV file

* use the <csvheader:hasheader|noheader> parameter to skip header

**sample:**

```
java -jar cals.jar <schemaFile> <controlFile> <dataFile> <outPutFolder> hasheader
```

```
Date,Open,High,Low,Close,Volume,Adj Close
2015-03-25,570.50,572.26,558.74,558.78,2110700,558.78
2015-03-24,562.56,574.59,561.21,570.19,2570100,570.19
2015-03-23,560.43,562.36,555.83,558.81,1625600,558.81
2015-03-20,561.65,561.72,559.05,560.36,2585800,560.36
```


```
java -jar cals.jar <schemaFile> <controlFile> <dataFile> <outPutFolder> noheader
```

```
2015-03-25,570.50,572.26,558.74,558.78,2110700,558.78
2015-03-24,562.56,574.59,561.21,570.19,2570100,570.19
2015-03-23,560.43,562.36,555.83,558.81,1625600,558.81
2015-03-20,561.65,561.72,559.05,560.36,2585800,560.36
```