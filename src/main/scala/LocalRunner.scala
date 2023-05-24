package task

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType, StringType, StructField, StructType}

import scala.io.Source

object LocalRunner {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder
        .master("local[*]")
        .appName("task")
        .config("spark.eventLog.enabled", false)
        .getOrCreate()

    val source = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .option("dateFormat", "M/d/yyyy")
      .schema(csvSchema)
      .csv("src/main/resources/input.csv")

    source
      .createOrReplaceTempView("source")

    val result3 = spark.sql(Source.fromFile("src/main/resources/transform3.sql").mkString)
    result3.show(false)

    result3
      .createOrReplaceTempView("input")

    val result4a = result3.sqlContext
      .sql(Source.fromFile("src/main/resources/transform4a.sql").mkString)
    result4a.show(false)

    val result4b = result3.sqlContext
      .sql(Source.fromFile("src/main/resources/transform4b.sql").mkString)
    result4b.show(false)


    val result4c = result3.sqlContext
      .sql(Source.fromFile("src/main/resources/transform4c.sql").mkString)
    result4c.show(false)

    val result4d = result3.sqlContext
      .sql(Source.fromFile("src/main/resources/transform4d.sql").mkString)
    result4d.show(false)

    spark.stop()
  }

  val csvSchema: StructType = StructType(Array(
    StructField("msisdn", StringType,  false),
    StructField("subscr_id", IntegerType,  false),
    StructField("email_address", StringType,  false),
    StructField("age", StringType,  true),
    StructField("join_date", DateType,  true),
    StructField("gender", StringType,  true),
    StructField("postal_sector", StringType,  true),
    StructField("handset_model", StringType,  true),
    StructField("handset_manufacturer", StringType,  true),
    StructField("needs_segment_name", StringType,  true),
    StructField("smart_phone_ind", StringType,  true),
    StructField("operating_system_name", StringType,  true),
    StructField("lte_subscr_ind", StringType,  true),
    StructField("bill_cycle_day", IntegerType,  true),
    StructField("avg_3_mths_spend", DecimalType(10, 0),  true),
    StructField("avg_3_mths_calls_usage", IntegerType,  true),
    StructField("avg_3_mths_sms_usage", IntegerType,  true),
    StructField("avg_3_mths_data_usage", IntegerType,  true),
    StructField("avg_3_mths_intl_calls_usage", IntegerType,  true),
    StructField("avg_3_mths_roam_calls_usage", IntegerType,  true),
    StructField("avg_3_mths_roam_sms_usage", IntegerType,  true),
    StructField("avg_3_mths_roam_data_usage", IntegerType,  true),
    StructField("data_bolton_ind", StringType,  true),
    StructField("insurance_bolton_ind", StringType,  true),
    StructField("o2travel_optin_ind", StringType,  true),
    StructField("pm_registered_ind", StringType,  true),
    StructField("connection_dt", DateType,  true),
    StructField("contract_start_dt", DateType,  true),
    StructField("contract_end_dt", DateType,  true),
    StructField("contract_term_mths", IntegerType,  true),
    StructField("upgrade_dt", DateType,  true),
    StructField("cust_tenure_mths", IntegerType,  true),
    StructField("pay_and_go_migrated_ind", StringType,  true),
    StructField("pay_and_go_migrated_dt", DateType,  true),
    StructField("ported_in_ind", StringType,  true),
    StructField("ported_in_dt", DateType,  true),
    StructField("ported_in_from_netwk_name", StringType,  true),
    StructField("disconnection_dt", DateType,  true),
    StructField("tariff_name", StringType,  true),
    StructField("sim_only_ind", StringType,  true),
    StructField("acquisition_channel_name", StringType,  true),
    StructField("billing_system_name", StringType,  true),
    StructField("last_billing_date", DateType,  true),
    StructField("event_desc", StringType,  true),
    StructField("contact_event_type_cd", StringType,  true),
    StructField("event_start_dt", DateType,  true),
    StructField("campaign_cd", StringType,  true),
    StructField("texts_optin_ind", StringType,  true),
    StructField("email_optin_ind", StringType,  true),
    StructField("phone_optin_ind", StringType,  true),
    StructField("post_optin_ind", StringType,  true),
    StructField("all_marketing_optin_ind", StringType, true)
  ))
}
