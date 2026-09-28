import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day27BankingStreaming {

  def main(args: Array[String]): Unit = {

    println()
    println("==============================================")
    println("        DAY 27 - REAL-TIME BANKING")
    println("==============================================")

    val conf = new SparkConf()
      .setAppName("Day27BankingStreaming")
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("OFF")
    ssc.checkpoint("/tmp/day27-banking-checkpoint")

    // ------------------------------------------------
    // 1. TRANSACTION EVENT STREAM
    // Format:
    // transactionId,accountId,branchId,amount
    // ------------------------------------------------

    val lines = ssc.socketTextStream("localhost", 9999)

    val transactions = lines.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 4) {
        try {
          Some(
            (
              parts(0).trim,
              parts(1).trim,
              parts(2).trim,
              parts(3).trim.toDouble
            )
          )
        } catch {
          case _: Exception => None
        }
      } else {
        None
      }
    }

    // ------------------------------------------------
    // 2. CACHE / PERSIST
    // ------------------------------------------------

    val persistedTransactions =
      transactions.persist(StorageLevel.MEMORY_AND_DISK)

    // ------------------------------------------------
    // 3. DISPLAY TRANSACTIONS
    // ------------------------------------------------

    persistedTransactions.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==============================================")
        println("          BANK TRANSACTION EVENTS")
        println("==============================================")

        rdd.collect().foreach {

          case (transactionId, accountId, branchId, amount) =>

            println(
              f"Transaction: $transactionId | " +
              f"Account: $accountId | " +
              f"Branch: $branchId | " +
              f"Amount: $amount%.2f"
            )
        }

        println("==============================================")
      }
    }

    // ------------------------------------------------
    // 4. AGGREGATE TRANSACTIONS BY ACCOUNT
    // ------------------------------------------------

    val accountTotals =
      persistedTransactions
        .map {
          case (_, accountId, _, amount) =>
            (accountId, amount)
        }
        .reduceByKey(_ + _)

    accountTotals.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==============================================")
        println("       ACCOUNT TRANSACTION TOTALS")
        println("==============================================")

        rdd.collect()
          .sortBy(_._1)
          .foreach {

            case (account, total) =>
              println(f"$account -> $total%.2f")
          }

        println("==============================================")
      }
    }

    // ------------------------------------------------
    // 5. SMALL BRANCH / RISK REFERENCE DATA
    // ------------------------------------------------

    val branchRiskData = Map(
      "B001" -> ("Hyderabad", "LOW"),
      "B002" -> ("Bangalore", "MEDIUM"),
      "B003" -> ("Chennai", "LOW"),
      "B004" -> ("Mumbai", "HIGH"),
      "B005" -> ("Delhi", "MEDIUM")
    )

    val broadcastBranchRisk =
      ssc.sparkContext.broadcast(branchRiskData)

    // ------------------------------------------------
    // 6. JOIN TRANSACTIONS WITH REFERENCE DATA
    // ------------------------------------------------

    val enrichedTransactions =
      persistedTransactions.map {

        case (transactionId, accountId, branchId, amount) =>

          val branchInfo =
            broadcastBranchRisk.value.getOrElse(
              branchId,
              ("Unknown", "UNKNOWN")
            )

          (
            transactionId,
            accountId,
            branchId,
            branchInfo._1,
            branchInfo._2,
            amount
          )
      }

    enrichedTransactions.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==============================================")
        println("       ENRICHED BANK TRANSACTIONS")
        println("==============================================")

        rdd.collect().foreach {

          case (
                transactionId,
                accountId,
                branchId,
                branchName,
                riskLevel,
                amount
              ) =>

            println(
              f"$transactionId | $accountId | $branchId | " +
              f"$branchName%-10s | Risk: $riskLevel%-6s | " +
              f"Amount: $amount%.2f"
            )
        }

        println("==============================================")
      }
    }

    // ------------------------------------------------
    // 7. SUSPICIOUS TRANSACTION BURSTS
    // 10-second window, sliding every 5 seconds
    // ------------------------------------------------

    val accountTransactionStream =
      persistedTransactions.map {
        case (_, accountId, _, _) =>
          (accountId, 1)
      }

    val suspiciousBursts =
      accountTransactionStream.reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(10),
        Seconds(5)
      )

    suspiciousBursts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==============================================")
        println("       SUSPICIOUS TRANSACTION BURSTS")
        println("==============================================")

        rdd.collect()
          .sortBy(_._1)
          .foreach {

            case (account, count) =>

              println(
                s"Account: $account | Transactions in window: $count"
              )

              if (count >= 3) {
                println(
                  s"🚨 SUSPICIOUS BURST ALERT: $account"
                )
              }
          }

        println("==============================================")
      }
    }

    // ------------------------------------------------
    // 8. PARTITIONING
    // Partition the RDD directly inside foreachRDD.
    // ------------------------------------------------

    persistedTransactions.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val partitionedRDD =
          rdd
            .map {
              case (_, accountId, _, amount) =>
                (accountId, amount)
            }
            .partitionBy(new HashPartitioner(4))

        println()
        println("==============================================")
        println("             PARTITIONING")
        println("==============================================")
        println(
          "Number of partitions: " +
          partitionedRDD.getNumPartitions
        )
        println("Partitioning method : HashPartitioner")
        println("==============================================")
      }
    }

    // ------------------------------------------------
    // 9. YARN EXECUTION EXPLANATION
    // ------------------------------------------------

    println()
    println("==============================================")
    println("             YARN EXECUTION")
    println("==============================================")
    println("1. Submit the Spark application to YARN.")
    println("2. YARN ResourceManager accepts the application.")
    println("3. ApplicationMaster manages the application.")
    println("4. YARN allocates containers for executors.")
    println("5. Spark executors process partitions in parallel.")
    println("6. Spark Driver coordinates jobs and tasks.")
    println("7. YARN manages cluster resources.")
    println("==============================================")

    // ------------------------------------------------
    // 10. START STREAMING
    // ------------------------------------------------

    ssc.start()

    println()
    println("==============================================")
    println("       BANKING STREAMING STARTED")
    println("==============================================")
    println("Batch Interval : 5 seconds")
    println("Window Size    : 10 seconds")
    println("Slide Interval : 5 seconds")
    println("Socket         : localhost:9999")
    println()
    println("Input format:")
    println("transactionId,accountId,branchId,amount")
    println()
    println("Example:")
    println("T001,A001,B001,5000")
    println("T002,A001,B001,7000")
    println("T003,A002,B002,12000")
    println()
    println("Waiting for banking transactions...")
    println("==============================================")
    println()

    ssc.awaitTermination()
  }
}
