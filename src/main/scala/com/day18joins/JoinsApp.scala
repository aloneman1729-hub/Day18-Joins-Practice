package com.day18joins

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object JoinsApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day18 Joins Practice")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    /*
     * Disable broadcast and adaptive execution so that
     * Spark can demonstrate Shuffle Sort Merge Join
     * for the larger joins in this practice.
     */
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    // --------------------------------------------------
    // 1. READ DATA
    // --------------------------------------------------

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/customers.csv")

    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/orders.csv")

    val payments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/payments.csv")

    println("\n================ CUSTOMERS ================")
    customers.show(false)

    println("\n================ ORDERS ==================")
    orders.show(false)

    println("\n================ PAYMENTS ================")
    payments.show(false)

    // --------------------------------------------------
    // 2. INNER JOIN
    // Orders + Customers
    // --------------------------------------------------

    println("\n\n================ INNER JOIN ================")

    val innerJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "inner"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.amount")
      )

    innerJoin.show(false)

    // --------------------------------------------------
    // 3. LEFT JOIN
    // Orders + Customers
    // --------------------------------------------------

    println("\n\n================ LEFT JOIN ================")

    val leftJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "left"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.amount")
      )

    leftJoin.show(false)

    // --------------------------------------------------
    // 4. NULL HANDLING AFTER LEFT JOIN
    // --------------------------------------------------

    println("\n\n================ NULL HANDLING ================")

    val nullHandled = leftJoin
      .withColumn(
        "customer_name",
        coalesce(col("customer_name"), lit("Unknown Customer"))
      )
      .withColumn(
        "city",
        coalesce(col("city"), lit("Unknown City"))
      )

    nullHandled.show(false)

    // --------------------------------------------------
    // 5. RIGHT JOIN
    // Customers + Orders
    // --------------------------------------------------

    println("\n\n================ RIGHT JOIN ================")

    val rightJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "right"
      )
      .select(
        col("o.order_id"),
        col("c.customer_id").alias("customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.amount")
      )

    rightJoin.show(false)

    // --------------------------------------------------
    // 6. FULL OUTER JOIN
    // Orders + Customers
    // --------------------------------------------------

    println("\n\n================ FULL OUTER JOIN ================")

    val fullJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "full"
      )
      .select(
        col("o.order_id"),
        coalesce(
          col("o.customer_id"),
          col("c.customer_id")
        ).alias("customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.amount")
      )

    fullJoin.show(false)

    // --------------------------------------------------
    // 7. ORDERS + CUSTOMERS + PAYMENTS
    // Complete Business Scenario
    // --------------------------------------------------

    println("\n\n================ ORDERS + CUSTOMERS + PAYMENTS ================")

    val orderCustomer = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "left"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.amount")
      )

    val finalReport = orderCustomer.alias("oc")
      .join(
        payments.alias("p"),
        col("oc.order_id") === col("p.order_id"),
        "left"
      )
      .select(
        col("oc.order_id"),
        col("oc.customer_id"),
        col("oc.customer_name"),
        col("oc.city"),
        col("oc.product"),
        col("oc.amount"),
        col("p.payment_status"),
        col("p.paid_amount")
      )
      .withColumn(
  "customer_name",
  coalesce(col("customer_name"), lit("Unknown Customer"))
)
.withColumn(
  "city",
  coalesce(col("city"), lit("Unknown City"))
)
.withColumn(
  "payment_status",
  coalesce(col("payment_status"), lit("Payment Not Found"))
)
.withColumn(
  "paid_amount",
  coalesce(col("paid_amount"), lit(0))
)

    finalReport.show(false)

    // --------------------------------------------------
    // 8. BUSINESS ANALYSIS
    // --------------------------------------------------

    println("\n\n================ BUSINESS ANALYSIS ================")

    finalReport
      .groupBy("payment_status")
      .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_order_amount"),
        sum("paid_amount").alias("total_paid_amount")
      )
      .orderBy("payment_status")
      .show(false)

    // --------------------------------------------------
    // 9. SHUFFLE SORT MERGE JOIN
    // --------------------------------------------------

    println("\n\n================ SHUFFLE SORT MERGE JOIN PLAN ================")

    val sortMergeJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "inner"
      )

    sortMergeJoin.explain(true)

    spark.stop()
  }
}
