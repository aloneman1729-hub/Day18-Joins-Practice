# Day 18 — Spark Joins Practice

A Scala + Apache Spark project demonstrating different join operations using a realistic Orders, Customers, and Payments scenario.

## Topics Covered

- Inner Join
- Left Join
- Right Join
- Full Outer Join
- Handling ambiguous column names using aliases
- Null handling after LEFT JOIN
- Joining multiple DataFrames
- Business analysis after joins
- Shuffle Sort Merge Join
- Spark physical execution plan

## Project Structure

```text
Day18-Joins-Practice/
├── build.sbt
├── project/
│   └── build.properties
├── src/
│   └── main/
│       ├── scala/
│       │   └── com/
│       │       └── day18joins/
│       │           └── JoinsApp.scala
│       └── resources/
│           └── data/
│               ├── customers.csv
│               ├── orders.csv
│               └── payments.csv
├── .gitignore
└── README.md
