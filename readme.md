# Storch - Scala 3 Pandas-like DataFrame Library
## Introduction
Storch is a Scala 3 implementation inspired by Pandas, offering a powerful and flexible DataFrame library. It enables seamless interoperability with Python DataFrames and supports reading from and writing to a wide range of file formats, including Avro, Arrow, Parquet, CSV, database (DB), Excel, ODS, NPZ, NPY, PKL, PT, PTH, and HDF5. Additionally, it provides common DataFrame operations and is continuously evolving to support more features in the future.

## Features
### File Format Support
Read/Write: Avro, Arrow, Parquet, CSV, DB, Excel, ODS, NPZ, NPY, PKL, PT, PTH, HDF5.
### Python Interoperability
Exchange DataFrames between Scala and Python environments.
### DataFrame Operations
Common operations such as filtering, grouping, aggregation, and more.
### Installation
To use Storch in your project, add the following to your build.sbt file:

```scala 3
libraryDependencies += "io.github.mullerhai" % "storch-pandas_3" % "0.0.1"
```

```scala 3

import torch.DataFrame

@main
def main(): Unit =
  val csvPath = "D:\\data\\git\\storch-pandas-old-use\\src\\main\\resources\\sample.csv"
  val df = DataFrame.readCsv(csvPath)
  println(df)
```

```scala 3
import torch.DataFrame

@main
def main(): Unit =
  val xlsPath = "D:\\data\\git\\storch-pandas-old-use\\src\\main\\resources\\sample_new2.xls"
  val xlsdf = DataFrame.readXls(xlsPath)
  println(xlsdf)
  
```


```scala 3
import torch.DataFrame
import scala.collection.mutable
import scala.collection.immutable.Set as KeySet

@main
def main(): Unit =
  val cols: mutable.Set[Any] = mutable.Set("category", "name", "value")
  val rows: KeySet[Any] = KeySet("row1", "row2", "row3")
  val col1Data = Seq("test", "test", "test", "beta", "beta", "beta")
  val col2Data = Seq("one", "two", "three", "one", "two", "three")
  val col3Data = Seq(10, 20, 30, 40, 50, 60)
  val data = List(col1Data, col2Data, col3Data)

  val df = new DataFrame[String](rows, cols, data)

  // Print the first 6 rows
  println(df.heads(6))

  // Print column names
  println(df.getColumns.mkString(","))
```

## Python Interoperability
While specific examples of Python interoperability aren't fully shown in the test files, Storch is designed to allow smooth DataFrame exchange between Scala and Python. You can use serialization libraries like pickle in Python and corresponding deserialization in Scala to achieve this.

## Supported File Formats
| Format  | Read | Write | 
|---------|------|-------|
| Avro    | ✔ | ✔ |
| Arrow   | ✔ | ✔ |
| Parquet | ✔ | ✔ | 
| CSV     | ✔ | ✔ | 
| DB      | ✔ | ✔ | 
| Excel   | ✔ | ✔ |
| ODS     | ✔ | ✔ | 
| NPZ     | ✔ | ✔ | 
| NPY     | ✔ | ✔ | 
| PKL     | ✔ | ✔ | 
| PT      | ✔ | ✔ | 
| PTH     | ✔ | ✔ |
| HDF5    | ✔ | ✔ |
| msgpack | ✔ | ✔ |
## Future Development
We plan to enhance Storch by adding more features, including support for additional file formats, performance optimizations, and advanced DataFrame operations.

## Contributing
Contributions are welcome! If you'd like to contribute, please follow these steps:

Fork the repository.
Create a new branch for your feature or bug fix.
Submit a pull request with a detailed description of your changes.
For major changes, please open an issue first to discuss your ideas.

## License
This project is licensed under the Apache 2.0 License - see the LICENSE file for details.