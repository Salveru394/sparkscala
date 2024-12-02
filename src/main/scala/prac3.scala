import io.netty.handler.codec.smtp.SmtpRequests.data
import jdk.nashorn.internal.objects.NativeString.substr
import org.apache.avro.LogicalTypes.date
import org.apache.avro.SchemaBuilder.nullable
import org.apache.hadoop.io.nativeio.NativeIO.link
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{avg, broadcast, callUDF, col, collect_list, count, countDistinct, date_format, datediff, dayofmonth, dense_rank, desc, initcap, lag, lead, max, min, month, rank, regexp_extract, regexp_replace, row_number, size, sum, to_date, to_timestamp, unix_timestamp, upper, when, window, year}
import sun.misc.MessageUtils.where
import org.apache.spark.sql.expressions.Window

import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalQueries.offset
import scala.Console.{in, println, withIn}
import scala.reflect.internal.TreesStats.nodeByType.result

object prac3 {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()

    //    val df = spark.read
    //      .format("csv")
    //      .option("header", "true")
    ////      .load("C:/Users/91957/Downloads/details.csv")

  }

//    val dfRepartitioned = df.repartition(2)
//
//    dfRepartitioned
//      .write
//      .format("csv")
//      .partitionBy("city")
//      .option("header", "true")
//      .save("C:/Users/91957/Documents/may5.txt")
//
//  }

    //    val sparkconf=new SparkConf()
    //        sparkconf.set("spark.appname","spark-program")
    //        sparkconf.set("spark.master","local[*]")
    //
    //      val spark=SparkSession.builder()
    //        .config(sparkconf)
    //        .getOrCreate()


    //  val schema=" id Int,Name String,Age Int,Salary Int,city String,details String,mean Int"

    //val schema=StructType(List(
    //StructField("id",IntegerType),
    //StructField("Name",StringType),
    //StructField("Age",IntegerType),
    //StructField("Salary",IntegerType),
    //StructField("city",StringType),
    //StructField("details",StringType),
    //StructField("mean",IntegerType)

    //))

    //val df=spark.read
    //.format("csv")
    //.option("header",true)
    //.schema(schema)
    //.option("path","C:/Users/91957/Documents/info.csv")
    //.load()

    //var employees = List((1,25,30000),(2,45,50000),(3,35,40000)).toDF("employee_id","age","salary")
    //when(col("age")<30 && col("salary")<35000,"young & low salary")
    // .when(col("age") >=30 && col("age") <=40 && col("salary") >=35000 && col("salary") <=45000,"middle aged & medium salary")
    //.otherwise("old & high salary")



    //var employees = List((1, 25,30000),(2, 45,50000),(3, 35,40000)).toDF("employee_id","age","salary")
    //var updateemp = employees.withColumn("category",
     // when(col("age") < 30 && col("salary") < 35000,"Young & Low salary")
      //  .when(col("age").between(30,40) && col("salary").between(35000,45000),"Middle Aged & Medium Salary")
       // .otherwise("Old & High Salary")).show()

    //var data = List(("mohan",78),("Ajay",90)).toDF("Name","Age")
    //data.show()

    //df.select(col("review_id"),col("rating"),
    //when(col("rating") < 3,"Bad").when(col("rating") >= 3 || col("rating") <= 4 && col("rating") <5, "Excellent")
    //.otherwise("feedback").alias("category")).show()




    //import spark.implicits._
    //var ratingsData= Seq(("user1", "Movie1", 4.5),("User2", "Movie1", 3.5),
    //("User3", "Movie2", 2.5),
    //("User4", "Movie2", 3.0),
    //("User1", "Movie3", 5.0),
    //("User2", "Movie3", 4.0)
    //).toDF("User", "Movie", "Rating")
    //ratingsData.groupBy(col("Movie")).agg( avg(col("Rating")) , count(col("Total_Rating")) ).show()
    //var averageRatings = ratingsData.groupBy("Movie").agg( avg("Rating").as("Average_rating"), count("Rating").as("Total_Ratings"))
    //averageRatings.show()

   // val scoreData = Seq(("Alice", "Math", 80), ("Bob", "Math", 90), ("Alice", "Science", 70),
    // ("Bob", "Science", 85), ("Alice", "English", 75), ("Bob", "English", 95)
   //).toDF("Student", "Subject", "Score")
   //val averagescore = scoreData.groupBy("Student").agg( avg("Score").as("Average_score"), max("Score").as("maximum_score"))
    //averagescore.show()



    //    val salesData = Seq(("Product1", "Category1", 100), ("Product2", "Category2", 200), ("Product3", "Category1", 150), ("Product4", "Category3", 300), ("Product5", "Category2", 250), ("Product6", "Category3", 180) //
//     ).toDF("Product", "Category", "Revenue")
//    val window=Window.partitionBy("Category").orderBy("Product")
//    val df = salesData.withColumn("total_revenue",sum(col("Revenue")).over(window))
//    df.show()

//val ratingData = Seq(("User1", "Movie1", 4.5), ("User1", "Movie2", 3.5),
 //("User1", "Movie3", 2.5), ("User1", "Movie4", 4.0),
 //("User1", "Movie5", 3.0), ("User1", "Movie6", 4.5),
 //("User2", "Movie1", 3.0), ("User2", "Movie2", 4.0),
 //("User2", "Movie3", 4.5),
 //("User2", "Movie4", 3.5),
 //("User2", "Movie5", 4.0),
 //("User2", "Movie6", 3.5)
//).toDF("User", "Movie", "Rating")
//var window = Window.partitionBy("User").orderBy(desc("Movie")).rowsBetween(0,2)
//var df = ratingData.withColumn("Averagerating" , avg(col("Rating")).over(window)) //
    // df.show()

    //val ratingData = Seq(
     // ("User1", "Movie1", 4.5), ("User1", "Movie2", 3.5), ("User1", "Movie3", 2.5), ("User1", "Movie4", 4.0), ("User1", "Movie5", 3.0),("User1", "Movie6", 4.5),
     // ("User2", "Movie1", 3.0), ("User2", "Movie2", 4.0), ("User2", "Movie3", 4.5), ("User2", "Movie4", 3.5), ("User2", "Movie5", 4.0), ("User2", "Movie6", 3.5)).toDF("User", "Movie", "Rating")
    //val windowSpec = Window.partitionBy("User").orderBy("Rating")
    //val averageRating = ratingData.withColumn("leadnewcolumn",lag("Rating",1).over(windowSpec))
    //averageRating.show()

//val shopData = List((1, "KitKat", 1000.0,"2021-01-01"),
//      (1, "KitKat", 2000.0,"2021-01-02"), (1, "KitKat", 1000.0,"2021-01-03"), (1, "KitKat", 2000.0,"2021-01-04"), (1, "KitKat", 3000.0,"2021-01-05"), (1, "KitKat", 1000.0,"2021-01-06")
//    ).toDF("IT_ID", "IT_Name", "Price","PriceDate")
//    val windowSpec = Window.partitionBy("IT_ID").orderBy("PriceDate")
//    val averageRating = shopData.withColumn("leadnewcolumn",lead("Price",1).over(windowSpec))
//    averageRating.show()

    //val df1=spark.read.format("csv").option("header",true).option("path","C:/Users/91957/Downloads/info.csv").load()
    //val df2=spark.read.format("csv").option("header",true).option("path","C:/Users/91957/Downloads/details.csv").load()
//    val condition=df1("id")===df2("id")
//    val jointype="inner"
//    val  joineddf=df1.join(broadcast(df2),condition,jointype).drop(df1("id"))
//    joineddf.show()

//    val df = List(("2023-10-07", "15:30:00")).toDF("date_str", "time_str")
//    val formattedDf = df.withColumn("date", to_date(col("date_str")))
//      .withColumn("time", to_timestamp(col("time_str")))
//    formattedDf.show()
//    formattedDf.printSchema()

//   val df = List(("07-10-2023", "15:30:00")).toDF("date_str", "time_str")
//     df.show()
//    df.printSchema()
//  val formattedDf = df.withColumn("date", to_date(col("date_str"),"dd-MM-yyyy"))
//   .withColumn("time", to_timestamp(col("time_str")))
//    formattedDf.show()
//    formattedDf.printSchema()

    // 27 th question
//    val leave_record = List((1,"Sick",12,"2024-01-10"),(2,"Sick",7,"2024-01-15"),(3,"Sick",3,"2024-02-20"),(4,"Sick",6,"2024-02-25"),(5,"Sick",2,"2024-03-05"),(6,"Casual",5,"2024-03-12"))
//      .toDF("employee_id","leave_type", "leave_duration_days", "leave_date")
//    val recorddf = leave_record.withColumn("leave_category",when(col("leave_duration_days")>10,"Extended").when(col("leave_duration_days")<=5 && col("leave_duration_days")<=10,"Short").otherwise("Casual"))
//    recorddf.show()
//    leave_record.filter(col("leave_type").startsWith("Sick"))
//    val recordgrp = recorddf.groupBy("leave_category")
//    recordgrp.agg( sum("leave_duration_days")).as("total_leave_duration_days").show()
//    recordgrp.agg( max("leave_duration_days")).as("maximum_leave_duration_days").show()
//    recordgrp.agg( min("leave_duration_days")).as("minimum_leave_duration_days").show()
//    recordgrp.agg( avg("leave_duration_days")).as("average_leave_duration_days").show()

   // 28th question
//   val sponsorships = List((1, "Alpha Corp", 12000, "2024-02-05"),(2, "Beta LLC", 7000, "2024-03-10"),(3, "Gamma Inc", 3000, "2024-04-15"),(4, "Delta Ltd", 9000, "2024-05-20"),(5, "Epsilon Co", 15000, "2024-02-25"),(6, "Zeta AG", 4000, "2024-03-28"))
//      .toDF("sponsor_id", "sponsor_name", "sponsorship_amount", "sponsorship_date")
//    val filteredSponsorships = sponsorships.filter(month(col("sponsorship_date")) ===2 && year(col("sponsorship_date")) ===2024)
//    val eventdf = filteredSponsorships.withColumn("amount_category",when(col("sponsorship_amount")>10000,"High").when(col("sponsorship_amount")>=5000 && col("sponsorship_amount")<=10000,"Medium").otherwise("Low"))
//    eventdf.show()
//    val eventgrp = eventdf.groupBy("amount_category")
//    eventgrp.agg( avg("sponsorship_amount")).as("average_sponsorship_amount").show()
//    eventgrp.agg( min("sponsorship_amount")).as("minimum_sponsorship_amount").show()
//    eventgrp.agg( max("sponsorship_amount")).as("maximum_sponsorship_amount").show()
//    eventgrp.agg( sum("sponsorship_amount")).as("total_sponsorship_amount").show()

// 29th question
//    val monthly_sales = List((1, "Widget",12000,"2024-04-01"),(2,"Gadget",6000,"2024-04-05"),(3,"Widget Pro",3000,"2024-04-10"),(4,"Gadget Pro",8000,"2024-04-15"),(5,"Widget Max",15000,"2024-04-20"),(6,"Gadget Max",4000,"2024-04-25"))
//      .toDF("sales_id","product_name","sales_amount","sales_date")
//    val monthly = monthly_sales.filter(month(col("sales_date"))===4 && year(col("sales_date")) === 2024)
//    val salesdf = monthly.withColumn("sales_performance",when(col("sales_amount")>10000,"Excellent").when(col("sales_amount")<=5000 && col("sales_amount")<=10000,"Good").otherwise("poor"))
//    salesdf.show()
//    val salesgrp = salesdf.groupBy("sales_performance")
//    salesgrp.agg( sum("sales_amount")).as("total_sales_performance").show()
//    salesgrp.agg( avg("sales_amount")).as("total_sales_performance").show()
//    salesgrp.agg( min("sales_amount")).as("total_sales_performance").show()
//    salesgrp.agg( max("sales_amount")).as("total_sales_performance").show()

// 30th  question
//    val product_return = List((1,"Widget Pro",6000,"2024-05-01"),(2,"Gadget Pro",3000,"2024-05-05"),(3,"Widget Max",1500,"2024-05-10"),(4,"Gadget Max",2500,"2024-05-15"),(5,"Widget Pro",7000,"2024-05-20"),(6,"Gadget Max",1000,"2024-05-25"))
//      .toDF("return_id","product_name","return_amount","return_date")
//     product_return.filter(col("product_name").endsWith("Pro"))
//    val productdf = product_return.withColumn("return_status",when(col("return_amount")>5000,"High").when(col("return_amount")<=2000 && col("return_amount")<=5000,"Medium").otherwise("Low"))
//    productdf.show()
//    val productgrp = productdf.groupBy("return_status")
//    productgrp.agg( sum("return_amount")).as("total_return_amount").show()
//    productgrp.agg( avg("return_amount")).as("average_return_amount").show()
//    productgrp.agg( min("return_amount")).as("minimum_return_amount").show()
//    productgrp.agg( max("return_amount")).as("maximum_return_amount").show()

//  31th question
//    val suppliers = List((1,"Alpha Ltd",16000,"2024-06-01"),(2,"Beta Inc",8000,"2024-06-05"),(3,"Gamma LLC",4000,"2024-06-10"),(4,"Delta Co",12000,"2024-06-15"),(5,"Epsilon Ltd", 18000, "2024-06-20"),(6, "Zeta Corp", 3000, "2024-06-25"))
//      .toDF("transaction_id","supplier_name","transaction_amount","transaction_date")
//    val supply = suppliers.filter(month(col("transaction_date")) ===6 && year(col("transaction_date")) === 2024)
//     val supplydf = supply.withColumn("transaction_status",when(col("transaction_amount")>15000,"High").when(col("transaction_amount")<=5000 && col("transaction_amount")<=15000,"Medium").otherwise("Low"))
//    supplydf.show()
//    val supplygrp = supplydf.groupBy("transaction_status")
//    supplygrp.agg( sum("transaction_amount")).as("total_transaction_amount").show()
//    supplygrp.agg( avg("transaction_amount")).as("average_transaction_amount").show()
//    supplygrp.agg( max("transaction_amount")).as("maximum_transaction_amount").show()
//    supplygrp.agg( min("transaction_amount")).as("minimum_transaction_amount").show()

// 32th question
//    val training = List((1,"HR",3500,"2024-07-01"),(2,"IT",1200,"2024-07-05"),(3,"HR",600,"2024-07-10"),(4,"HR",2500,"2024-07-15"),(5,"IT",800,"2024-07-20"),(6,"HR",4000,"2024-07-25"))
//      .toDF("expense_id","department","expense_amount","expense_date")
//    training.filter(col("department").startsWith("HR"))
//    val traindf = training.withColumn("expense_category",when(col("expense_amount")>3000,"High").when(col("expense_amount")<=1000 && col("expense_amount")<=3000,"Medium").otherwise("Low"))
//    traindf.show()
//    val traingrp = traindf.groupBy("expense_category")
//    traingrp.agg ( sum("expense_amount")).as("Total_expense_amount").show()
//    traingrp.agg ( min("expense_amount")).as("Total_expense_amount").show()
//    traingrp.agg ( max("expense_amount")).as("Total_expense_amount").show()
//    traingrp.agg ( avg("expense_amount")).as("Total_expense_amount").show()

    // 33th question
//    val customer_purchases = List((1, 1, 2500, "2024-08-01"),(2, 2, 1500, "2024-08-05"),(3, 3, 800, "2024-08-10"),(4, 4, 3000, "2024-08-15"),(5, 5, 1200, "2024-08-20"),(6, 6, 5000, "2024-08-25"))
//      .toDF("purchase_id", "customer_id", "purchase_amount", "purchase_date")
//    val custom = customer_purchases.filter(month(col("purchase_date"))===8 && year(col("purchase_date"))===2024)
//    val customerdf = custom.withColumn("purchase_level",when(col("purchase_amount")>2000,"High").when(col("purchase_amount")<=1000 && col("purchase_amount")<=2000,"Medium").otherwise("Low"))
//    customerdf.show()
//    val customergrp = customerdf.groupBy("purchase_level")
//    customergrp.agg( avg("purchase_amount")).as("average_purchase_amount").show()
//    customergrp.agg( min("purchase_amount")).as("minimum_purchase_amount").show()
//    customergrp.agg( max("purchase_amount")).as("maximum_purchase_amount").show()
//    customergrp.agg( sum("purchase_amount")).as("total_purchase_amount").show()

  // 34th question
//    val project_exp = List((1,"Development Project",8000,"2024-09-01"),(2, "Development Plan",4500,"2024-09-05"),(3,"Marketing Campaign",2500,"2024-09-10"),(4,"Development Phase",3000,"2024-09-15"),(5,"Development Task",10000,"2024-09-20"),(6,"R&D Project", 1500, "2024-09-25"))
//      .toDF("expense_id","project_name","expense_amount","expense_date")
//      val project = project_exp.filter(col("project_name").contains("Development"))
//    val projectdf = project.withColumn("expense_type",when(col("expense_amount")>7000,"High").when(col("expense_amount")<=3000 && col("expense_amount")<=7000,"Medium").otherwise("Low"))
//    projectdf.show()
//    val projectgrp = projectdf.groupBy("expense_type")
//    projectgrp.agg( sum("expense_amount")).as("total_expense_amount").show()
//    projectgrp.agg( min("expense_amount")).as("minimum_expense_amount").show()
//    projectgrp.agg( max("expense_amount")).as("maximum_expense_amount").show()
//    projectgrp.agg( avg("expense_amount")).as("average_expense_amount").show()

// 35th question
//    val rating_category = List((1,95,"2024-10-01"),(2,85,"2024-10-05"),(3,65,"2024-10-10"),(4,75,"2024-10-15"),(5,90,"2024-10-20"),(6,80,"2024-10-25"))
//      .toDF("employee_id","rating_score","rating_date")
//    val ratingdf0 = rating_category.filter(month(col("rating_date"))===10 && year(col("rating_date"))=== 2024)
//    val ratingdf1 = ratingdf0.withColumn("rating_category",when(col("rating_score")>=90,"Excellent").when(col("rating_score")<=70 &&  col("rating_score")<90,"Good").otherwise("Needs Improvement"))
//    ratingdf1.show()
//    val ratinggrp = ratingdf1.groupBy("rating_category")
//    ratinggrp.agg( count("rating_category")).as("total_rating_category").show()

    // 36th question
//    val campaign = List((1,"Summer Blast",22000,"2024-11-01"),(2,"Summer Sale",12000,"2024-11-05"),(3,"Winter Campaign",8000,"2024-11-10"),(4,"Summer Special",15000,"2024-11-15"),(5,"Winter Special",9000,"2024-11-20"),(6,"Summer Promo",25000,"2024-11-25"))
//      .toDF("campaign_id","campaign_name","expense_amount","expense_date")
//    val campaigndf = campaign.filter(col("campaign_name").startsWith("Summer"))
//    val campaigndf1 = campaigndf.withColumn("expense_type",when(col("expense_amount")>20000,"High").when(col("expense_amount")<=10000 && col("expense_amount")<=20000,"Medium").otherwise("Low"))
//    campaigndf1.show()
//    val windowSpec = Window.partitionBy("expense_type").orderBy("expense_amount")
//    val campaigngrp = campaigndf1.withColumn("total",sum(col("expense_amount")).over(windowSpec))
//               .withColumn("average",avg(col("expense_amount")).over(windowSpec))
//               .withColumn("maximum",max(col("expense_amount")).over(windowSpec))
//               .withColumn("minimum",min(col("expense_amount")).over(windowSpec))
//       campaigngrp.show()

// 37th question
//    val travel_expenses = List((1,1,5500,"2024-12-01"),(2,2,2500,"2024-12-05"),(3,3,1200,"2024-12-10"),(4,4,3000,"2024-12-15"),(5,5,4000,"2024-12-20"))
//      .toDF("expense_id","staff_id","travel_amount","travel_date")
//    val traveldf = travel_expenses.filter(month(col("travel_date"))===12 && year(col("travel_date"))===2024)
//    val traveldf1 = traveldf.withColumn("travel_category",when(col("travel_amount")>5000,"High").when(col("travel_amount")<=2000 && col("travel_amount")<=5000,"Medium").otherwise("Low"))
//    traveldf1.show()
//    val window = Window.partitionBy("travel_category").orderBy("travel_amount")
//    val travelgrp = traveldf1.withColumn("total",sum(col("travel_amount")).over(window))
//                .withColumn("average",avg(col("travel_amount")).over(window))
//               .withColumn("maximum",max(col("travel_amount")).over(window))
//               .withColumn("minimum",min(col("travel_amount")).over(window))
//     travelgrp.show()

    //38th question
//    val sales_commission = List((1, "Alice Johnson", 12000, "2025-01-01"),(2,"Bob Smith",7000,"2025-01-05"),(3, "Charlie Davis", 3000,"2025-01-10"),(4,"David Lee",8000,"2025-01-15"),(5,"Emily Clark",11000,"2025-01-20"),(6,"Frank Adams",2000,"2025-01-25"))
//      .toDF("sales_id","salesperson_name","commission_amount","commission_date")
//    val commissiondf = sales_commission.filter(month(col("commission_date")) ===1 && year(col("commission_date"))===2025)
//    val commissiondf1 = commissiondf.withColumn("commission_level",when(col("commission_amount")>10000,"High").when(col("commission_amount")<=5000 && col("commission_amount")<=10000,"Medium").otherwise("Low"))
//    commissiondf1.show()
//    val windowf_s = Window.partitionBy("commission_level").orderBy("commission_amount")
//    val commissiongrp = commissiondf1.withColumn("total",sum(col("commission_amount")).over(windowf_s))
//                 .withColumn("average",avg(col("commission_amount")).over(windowf_s))
//               .withColumn("maximum",max(col("commission_amount")).over(windowf_s))
//               .withColumn("minimum",min(col("commission_amount")).over(windowf_s))
//    commissiongrp.show()

// 39th question
//    val product_sales = List((1,"Tech Gadget",6000,"2025-02-01"),(2,"Tech Widget",3000,"2025-02-05"),(3,"Home Gadget",1500,"2025-02-10"),(4,"Tech Tool",5000,"2025-02-15"),(5,"Tech Device",7000,"2025-02-20"),(6,"Office Gadget",1800,"2025-02-25"))
//      .toDF("sale_id","product_name","sale_amount","sale_date")
//    val productdf = product_sales.filter(col("product_name").startsWith("Tech"))
//    val productdf1 = productdf.withColumn("sales_category",when(col("sale_amount")>5000,"High").when(col("sale_amount")<=2000 && col("sale_amount")<=5000,"Medium").otherwise("Low"))
//    productdf1.show()
//    val windows = Window.partitionBy("sales_category").orderBy("sale_amount")
//    val productgrp = productdf1.withColumn("Total",sum(col("sale_amount")).over(windows))
//              .withColumn("average",avg(col("sale_amount")).over(windows))
//               .withColumn("maximum",max(col("sale_amount")).over(windows))
//               .withColumn("minimum",min(col("sale_amount")).over(windows))
//       productgrp.show()

// 40th question
//    val custom_feedback = List((1,1,95,"2025-03-01"),(2,2,80,"2025-03-05"),(3,3,65,"2025-03-10"),(4,4,85,"2025-03-15"))
//      .toDF("feedback_id","customer_id","feedback_score","feedback_date")
//    val customdf = custom_feedback.filter(month(col("feedback_date"))===3 && year(col("feedback_date"))===2025)
//    val customdf1 = customdf.withColumn("feedback_rating",when(col("feedback_score")>=90,"Excellent").when(col("feedback_score")<=70 && col("feedback_score")<90,"Good").otherwise("Needs Improvement"))
//    customdf1.show()
//    val windowf = Window.partitionBy("feedback_rating")
//    val customgrp = customdf1.withColumn("total",count(col("feedback_rating")).over(windowf))
//    customgrp.show()

    // 41th question
//    val employees = List((1,"2024-09-01",9),(2, "2024-09-02" ,5),(3, "2024-09-03", 3),(4, "2024-09-04", 8),(5,"2024-09-05", 2),(6, "2024-09-06", 7))
//      .toDF("employee_id","attendance_date", "hours_worked")
//    val employeedf = employees.filter(month(col("attendance_date"))===9 && year(col("attendance_date"))===2024)
//    val emplyeedf1 = employeedf.withColumn("attendance_status",when(col("hours_worked")>=8,"Full Day").when(col("hours_worked")<=4 && col("hours_worked")<8,"Half Day").otherwise("Absent"))
//    emplyeedf1.show()
//    val employeegrp = emplyeedf1.groupBy("attendance_status")
//    employeegrp.agg( sum("hours_worked")).as("total_hours_worked").show()
//    employeegrp.agg( avg("hours_worked")).as("average_hours_worked").show()
//    employeegrp.agg( max("hours_worked")).as("maximum_hours_worked").show()
//    employeegrp.agg( min("hours_worked")).as("minimum_hours_worked").show()

    // 42th question
//    val project_costs = List((1,35000,"2025-04-01"),(2,20000,"2025-04-05"),(3,12000,"2025-04-10"),(4,25000,"2025-04-15"),(5,40000,"2025-04-20"),(6,14000,"2025-04-25"))
//      .toDF("project_id", "cost_amount", "allocation_date")
//    val projectdf = project_costs.filter(month(col("allocation_date"))===4 && year(col("allocation_date"))===2025)
//    val projectdf1 = projectdf.withColumn("cost_category",when(col("cost_amount")>30000,"High").when(col("cost_amount")<=15000 && col("cost_amount")<=3000,"Medium").otherwise("Low"))
//    projectdf1.show()
//    val windowd_f = Window.partitionBy("cost_category").orderBy("cost_amount")
//    val projectgrp = projectdf1.withColumn("total",sum(col("cost_amount")).over(windowd_f))
//      .withColumn("maximum",max(col("cost_amount")).over(windowd_f))
//      .withColumn("minimum",min(col("cost_amount")).over(windowd_f))
//      .withColumn("average",avg(col("cost_amount")).over(windowd_f))
//     projectgrp.show()

//  43th question
//    val sales_performance = List((1,25000,"2024-12-01"),(2,15000,"2024-12-05"),(3, 8000 ,"2024-12-10"),(4, 12000, "2024-12-15"),(5, 18000,"2024-12-20"),(6, 5000, "2024-12-25"))
//      .toDF("salesperson_id","sales_amount","sales_date")
//     val salesdf = sales_performance.filter(month(col("sales_date"))===12 && year(col("sales_date"))===2024)
//    val salesdf1 = salesdf.withColumn("performance_category",when(col("sales_amount")>20000,"Top performer").when(col("sales_amount")<=10000 && col("sales_amount")<=20000,"Average performance").otherwise("Low performance"))
//    salesdf1.show()
//    val window = Window.partitionBy("performance_category").orderBy("sales_amount")
//    val salesgrp = salesdf1.withColumn("total",sum(col("sales_amount")).over(window))
//             .withColumn("average",avg(col("sales_amount")).over(window))
//             .withColumn("maximum",max(col("sales_amount")).over(window))
//             .withColumn("minimum",min(col("sales_amount")).over(window))
//           salesgrp.show()

    // 44th question
//    val customer = List((1,12000,"2024-11-01"),(2,7000,"2024-11-05"),(3,3000,"2024-11-10"),(4,8000,"2024-11-15"),(5,15000,"2024-11-20"),(6,4000,"2024-11-25"))
//      .toDF("order_id","order_amount","order_date")
//    val customdf = customer.filter(month(col("order_date"))===11 && year(col("order_date"))===2024)
//    val customdf1 = customdf.withColumn("order_status",when(col("order_amount")>10000,"High Value").when(col("order_amount")<=5000 && col("order_amount")<=10000,"Medium").otherwise("Low Value"))
//    val customgrp = customdf1.groupBy("order_status")
//    customgrp.agg( sum("order_amount")).as("total_order_amount").show()
//    customgrp.agg( min("order_amount")).as("minimum_order_amount").show()
//    customgrp.agg( max("order_amount")).as("maximum_order_amount").show()
//    customgrp.agg( avg("order_amount")).as("average_order_amount").show()

   // 45th question
//    val emp_bonus = List((1,6000,"2025-01-01"),(2,2500,"2025-01-05"),(3,1500,"2025-01-10"),(4,3500,"2025-01-15"),(5,7000,"2025-01-20"),(6,1000,"2025-01-25"))
//      .toDF("employee_id","bonus_amount","bonus_date")
//    val  bonusdf = emp_bonus.filter(month(col("bonus_date"))===1 && year(col("bonus_date"))===2025)
//     val bonusdf1 = bonusdf.withColumn("bonus_level",when(col("bonus_amount")>5000,"High Bonus").when(col("bonus_amount")<=2000 && col("bonus_amount")<=5000,"Medium Bonus").otherwise("Low Bonus"))
//    val bonusgrp = bonusdf1.groupBy("bonus_level")
//    bonusgrp.agg( sum("bonus_amount")).as("total_bonus_amount").show()
//    bonusgrp.agg( avg("bonus_amount")).as("average_bonus_amount").show()
//    bonusgrp.agg( min("bonus_amount")).as("minimum_bonus_amount").show()
//    bonusgrp.agg( max("bonus_amount")).as("maximum_bonus_amount").show()

  // 46th question
//    val manufacturing = List((1,22000,"2025-02-01"),(2,15000,"2025-02-05"),(3,8000,"2025-02-10"),(4,12000,"2025-02-15"),(5,18000,"2025-02-20"),(6,9000,"2025-02-25"))
//      .toDF("product_id","manufacturing_cost","cost_date")
//    val manufacturerdf = manufacturing.filter(month(col("cost_date"))===2 && year(col("cost_date"))===2025)
//    val manufacturerdf1 = manufacturerdf.withColumn("cost_category",when(col("manufacturing_cost")>20000,"Expensive").when(col("manufacturing_cost")<=10000 && col("manufacturing_cost")<=20000,"Moderate").otherwise("Cheap"))
//    val window = Window.partitionBy("cost_category").orderBy("manufacturing_cost")
//    val mmanufacturedgrp = manufacturerdf1.withColumn("total",sum(col("manufacturing_cost")).over(window))
//          .withColumn("minimum",min(col("manufacturing_cost")).over(window))
//          .withColumn("maximum",max(col("manufacturing_cost")).over(window))
//          .withColumn("average",avg(col("manufacturing_cost")).over(window))

   //47th question
//    val dept_exp = List((1,"Finance",12000,"2025-03-01"),(2,"Finance",6000,"2025-03-05"),(3,"HR",3000,"2025-03-10"),(4,"Finance",8000,"2025-03-15"),(5,"Finance",15000,"2025-03-20"),(6,"IT",4000,"2025-03-25"))
//      .toDF("expense_id","department","expense_amount","expense_date")
//    val deptdf = dept_exp.filter(col("department").startsWith("Finance"))
//    val deptdf1 = deptdf.withColumn("expense_type",when(col("expense_amount")>10000,"High").when(col("expense_amount")<=5000 && col("expense_amount")<=10000,"Medium").otherwise("Low"))
//    val deptgrp = deptdf1.groupBy("expense_type")
//    deptgrp.agg( max("expense_amount")).as("maximum_expense_amount").show()
//    deptgrp.agg( min("expense_amount")).as("minimum_expense_amount").show()
//    deptgrp.agg( avg("expense_amount")).as("average_expense_amount").show()
//    deptgrp.agg( sum("expense_amount")).as("total_expense_amount").show()

   //48th question
//    val project_costs = List((1,35000,"2025-04-01"),(2,20000,"2025-04-05"),(3,12000,"2025-04-10"),(4,25000,"2025-04-15"))
//      .toDF("project_id","cost_amount","allocation_date")
//    val projectdf = project_costs.filter(month(col("allocation_date"))===4 && year(col("allocation_date"))===2025)
//    val projectdf1 = projectdf.withColumn("cost_category",when(col("cost_amount")>30000,"High").when(col("cost_amount")<=15000 && col("cost_amount")<=30000,"Medium").otherwise("Low"))
//    val windows = Window.partitionBy("cost_category").orderBy("cost_amount")
//    val projectgrp = projectdf1.withColumn("total",sum("cost_amount").over(windows))
//    .withColumn("minimum",min("cost_amount").over(windows))
//    .withColumn("maximum",max("cost_amount").over(windows))
//    .withColumn("average",avg("cost_amount").over(windows))
//    projectgrp.show()

    //49th question
//    val marketing_campaigns = List((1,27000,"2025-05-01"),(2,15000,"2025-05-05"),(3,7000,"2025-05-10"),(4,18000,"2025-05-15"))
//      .toDF("campaign_id","campaign_cost","campaign_date")
//    val marketdf = marketing_campaigns.filter(month(col("campaign_date"))===5 && year(col("campaign_date"))===2025)
//    val marketdf1 = marketdf.withColumn("performance_category",when(col("campaign_cost")>25000,"High").when(col("campaign_cost")<=10000 && col("campaign_cost")<=25000,"Medium").otherwise("Low"))
//    val window = Window.partitionBy("performance_category").orderBy("campaign_cost")
//    val marketgrp = marketdf1.withColumn("total",sum("campaign_cost").over(window))
//    .withColumn("maximum",max("campaign_cost").over(window))
//    .withColumn("minimum",min("campaign_cost").over(window))
//    .withColumn("average",avg("campaign_cost").over(window))
//    marketgrp.show()

   //50th question
//    val client_transactions = List((1, "TechCorp", 22000, "2025-06-01"),(2, "FinCorp", 12000, "2025-06-05"),(3,"RetailCorp", 8000,"2025-06-10"),(4,"TechCorp",15000,"2025-06-15"),(5,"TechCorp",25000,"2025-06-20"),(6,"FoodCorp",6000,"2025-06-25"))
//      .toDF("transaction_id","client_name","transaction_amount","transaction_date")
//    val clientdf = client_transactions.filter(col("client_name").endsWith("Corp"))
//    val clientdf1 = clientdf.withColumn("transaction_level",when(col("transaction_amount")>2000,"High").when(col("transaction_amount")<=10000 && col("transaction_amount")<=20000,"Medium").otherwise("Low"))
//    val windowes = Window.partitionBy("transaction_level").orderBy("transaction_amount")
//    val clientgrp = clientdf1.withColumn("total",sum("transaction_amount").over(windowes))
//    .withColumn("average",avg("transaction_amount").over(windowes))
//    .withColumn("minimum",min("transaction_amount").over(windowes))
//    .withColumn("maximum",max("transaction_amount").over(windowes))
//    clientgrp.show()


    //1st question
// Sample DataFrame
//val employee_performance = Seq(
//  ("E001", "Sales", 85, "2024-02-10", "Sales Manager"), ("E002", "HR", 78, "2024-03-15", "HR Assistant"),
//  ("E003", "IT", 92, "2024-01-22", "IT Manager"), ("E004", "Sales", 88, "2024-02-18", "Sales Rep"), ("E005", "HR", 95, "2024-03-20", "HR Manager")
//).toDF("employee_id", "department", "performance_score", "review_date", "position")
//val dfWithMonth = employee_performance.withColumn("review_month", month(col("review_date")))
//val filteredDF = dfWithMonth.filter(col("position").endsWith("Manager") && col("performance_score")> 80)
//val averagedf = filteredDF.groupBy("department", "review_month").agg(avg("performance_score").as("avg_performance_score"))
//val countAbove90DF = filteredDF.filter(col("performance_score") > 90)
//  .groupBy("department", "review_month")
//  .count()
//val windowSpec = Window.partitionBy("employee_id").orderBy("review_date")
//val performanceWithLag = dfWithMonth.withColumn("performance_improvement", lag("performance_score", 1).over(windowSpec))
//  .withColumn("performance_decline", col("performance_score") - col("previous_performance"))
//    averagedf.show()
//    countAbove90DF.show()
//performanceWithLag.show()


    // 2nd question
//    val customer_churn = Seq(("C001","Premium Gold","Yes","2023-12-01",1200,"USA" ),("C002","Basic","No","NULL",400,"Canada"),("C003","Premium Silver","Yes","2023-11-15",800,"UK"),("C004","Premium Gold","Yes","2024-01-10",1500,"USA"),("C005","Basic","No","NULL",300,"India"))
//  .toDF("customer_id","subscription_type","churn_status","churn_date","revenue","country")
//     val customerChurnWithYear = customer_churn.withColumn("churn_date", to_date(col("churn_date"), "yyyy-MM-dd"))
//  .withColumn("churn_year", year(col("churn_date")))
//    val filteredData = customerChurnWithYear.filter(col("subscription_type")
//  .startsWith("Premium") && col("churn_status").isNotNull)
//   val grouped = filteredData.groupBy("country", "churn_year")
//  .agg(sum("revenue").as("total_revenue_lost"), avg("revenue").as("avg_revenue_per_churned_customer"),
//    count("customer_id").as("count_of_churned_customers"))
//   val windowSpec = Window.partitionBy("country").orderBy("churn_year")
//   val nextYearRevenue = grouped
//  .withColumn("next_year_revenue_lost", lead("total_revenue_lost", 1).over(windowSpec))
//    val finalData = nextYearRevenue
//  .withColumn("revenue_trend", col("next_year_revenue_lost") - col("total_revenue_lost"))
//    finalData.show()


   // 3rd question
//    val sales_target = List(("S001",15000,12000,"2023-12-10", "North","Electronics Accessories"),("S002",8000,9000,"2023-12-11","South","Home Appliances"),("S003",20000,18000,"2023-12-12","East","Electronics Gadgets"),("S004",10000,15000,"2023-12-13","West","Electronics Accessories"))
//      .toDF("salesperson_id","sales_amount","target_amount","sale_date","region","product_category")
//    val targetdata =sales_target.withColumn("target_achieved",col("sales_amount") >= col("target_amount"))
//    val filteredData = targetdata.filter(col("product_category").startsWith("Electronics") && col("product_category").endsWith("Accessories"))
//    val grouped = filteredData.groupBy("region","product_category").agg(sum("sales_amount").as("total_sales_amount"),
//    min("sales_amount").as("min_sales_amount"),
//      sum(when(col("target_achieved"),1).otherwise(0)).as("num_salespersons_achieved_target")).show()
//    val windowspec = Window.partitionBy("salesperson_id").orderBy("sale_date")
//    val saleswithlag = targetdata.withColumn("previous_sales_amount",lag("sales_amount", 1).over(windowspec))
//    saleswithlag.show()


    //4th question
//    val loan_repayments = List(("L001","C001", 1000, "2023-11-01" ,"2023-12-01","Personal Loan","7.5%"),("L002","C002",2000,"2023-11-15","2023-11-20","Home Loan","6.5%"),("L003","C003",1500,"2023-12-01","2023-12-25", "Personal Loan","8.0%"),("L004", "C004", 2500,"2023-12-10","2024-01-15","Car Loan","9.0%"),("L005","C005",1200,"2023-12-15","2024-01-20","Personal Loan","7.8%"))
//      .toDF("loan_id","customer_id","repayment_amount","due_date","payment_date","loan_type","interest_rate")
//    val loanRepaymentsWithDelay = loan_repayments.withColumn("due_date", to_date(col("due_date"), "yyyy-MM-dd"))
//  .withColumn("payment_date", to_date(col("payment_date"), "yyyy-MM-dd"))
//  .withColumn("repayment_delay", datediff(col("payment_date"), col("due_date")))
//   val filteredData = loanRepaymentsWithDelay
//  .filter(col("loan_type").startsWith("Personal") && col("repayment_delay") > 30)
//  // Convert interest_rate to numeric format
//   val processedData = filteredData
//  .withColumn("interest_rate_numeric", regexp_replace(col("interest_rate"), "%", "").cast("double"))
//   val grouped = processedData
//  .groupBy("loan_type", "interest_rate_numeric")
//  .agg(
//    sum("repayment_amount").as("total_repayment_amount"),
//    max("repayment_delay").as("max_repayment_delay"),
//    avg("interest_rate_numeric").as("avg_interest_rate_for_delayed_payments"))
//  val windowSpec = Window.partitionBy("customer_id").orderBy("payment_date")
//  val loanWithLead = loanRepaymentsWithDelay
//  .withColumn("next_repayment_amount", lead("repayment_amount", 1).over(windowSpec))
//   grouped.show()
//   loanWithLead.show()

   // 5th question
//   val website_traffic = List(("S001","U001",15,"2023-10-01","Mobile","Organic"),("S002","U002",10,"2023-10-05","Desktop","Paid"),
//     ("S003","U003",20,"2023-10-10","Mobile","Organic"),("S004","U004",25,"2023-10-15","Tablet","Referral"),("S005","U001",30,"2023-11-01","Mobile","Organic"))
//     .toDF("session_id","user_id","page_views","session_date", "device_type", "traffic_source")
//    val sessiondata = website_traffic.withColumn("session_date", to_date(col("session_date"))).withColumn("session_month", month(col("session_date")))
//    val filterdata = sessiondata.filter(col("traffic_source") === "Organic" && col("device_type") === "Mobile")
//    val grouped = filterdata.groupBy("device_type","session_month").agg(
//      sum("page_views").as("total_page_views"),
//      avg("page_views").as("Avg_page_views_per_session"),
//      count("session_id").as("count_of_session"))
//    val windowdata = Window.partitionBy("user_id").orderBy("session_date")
//    val websitewithleadlag = sessiondata.withColumn("previous_page_views", lag("page_views",1).over(windowdata))
//      .withColumn("next_page_views", lead("page_views",1).over(windowdata))
//    websitewithleadlag.show()
//    grouped.show()

   // 6th question
//    val product_returns = List(("R001","O001","Electro Gadgets","2023-12-01","Damaged",100),("R002","O002","Home Appliances","2023-12-05","Defective",50),
//      ("R003","O003","Electro Toys","2023-12-10","Changed Mind",75),("R004","O004","Electro Gadgets","2023-12-15","Damaged",100),("R005","O005","Kitchen Set","2023-12-20","Wrong Product",120))
//      .toDF("return_id","order_id","product_name","return_date","return_reason","refund_amount")
//    val returndata = product_returns.withColumn("return_date", to_date(col("return_date"))).withColumn("return_year", year(col("return_date")))
//    val filterdata = returndata.filter(col("product_name").startsWith("Electro") && col("refund_amount").isNotNull)
//    val groupdata = filterdata.groupBy("return_reason","return_year").agg(
//      sum("refund_amount").as("Total_refund_amount"),
//      count("return_id").as("count_of_returns_for_each_reason"),
//      avg("refund_amount").as("Avg_refund_amount_per_return"))
//    val dataset = Window.partitionBy("product_name").orderBy("return_date")
//    val productlag = returndata.withColumn("previous_refund_amount", lag("refund_amount",1).over(dataset))
//    groupdata.show()
//    productlag.show()

    // 7th question
//    val patient_visits = Seq(("V001","P001","2023-11-01","Dr. Smith",700,"Cardiology"),("V002","P002","2023-11-10","Dr. Johnson",400,"Neurology"),
//      ("V003","P003","2023-12-01","Dr. Brown",900,"Cardiology"),("V004","P004","2023-12-15","Dr. Smith",600,"Cardiology"))
//      .toDF("visit_id","patient_id","visit_date","doctor_name","treatment_cost","department")
//    val visitdata = patient_visits.withColumn("visit_date", to_date(col("visit_date"),"yyyy-MM-dd")).withColumn("visit_year", year(col("visit_date")))
//    val filterdata = visitdata.filter(col("department").startsWith("Cardio") && col("treatment_cost") > 500)
//    val groupdata = filterdata.groupBy("doctor_name","visit_year").agg(
//      sum("treatment_cost").as("Total_treatment_cost"),
//      max("treatment_cost").as("Max_treatment_cost"),
//      count("visit_id").as("visit_count"))
//     val nextvisit = Window.partitionBy("patient_id").orderBy("visit_date")
//     val healthlead = patient_visits.withColumn("next_visit_treatment_cost",lead("treatment_cost",1).over(nextvisit))
//    groupdata.show()
//    healthlead.show()
//    filterdata.show()

// 8th question (not complete)(same like 11th)
//   val product_prices = Seq(("P001","Mobile Phone","2023-10-01",500,"Electronics"),("P002","Washing Machine","2023-10-05",700,"Home Appliances"),
//    ("P003","Laptop","2023-10-10",1200,"Electronics"),("P004","TV","2023-10-15",1000,"Consumer Electronics"),("P005","Mobile Phone","2023-11-01",550,"Electronics"))
//    .toDF("product_id","product_name","price_date","price", "category")
//   val pricedata = product_prices.withColumn("price_date", to_date(col("price_date"),"yyyy-MM-dd")).withColumn("price_month", month(col("price_date")))
//   val filterdata = pricedata.filter(col("category").endsWith("Electronics"))
//    //window specification for price comparision
//    val windows = Window.partitionBy("product_name").orderBy("price_month")
//    val price_with_lag = filterdata.withColumn("previous_price", lag("price", 1).over(windows))
//    .withColumn("price_increased", col("price") > col("previous_price")).filter(col("price_increased"))
//    val groupdata = filterdata.groupBy("product_name","price_month").agg(
//     avg("price").as("average_price"),max("price") - min("price").as("price_fluctuation"),
//    count("price").as("price changes"))
//    val windowSpec = Window.partitionBy("product_name").orderBy("price_month")
//    val pricewithlag = pricedata.withColumn("previous_price",lag("price", 1).over(windowSpec))
//      .withColumn("price_change_percentage", col("previous_price") -col("price"))
//    pricewithlag.show()
//    pricewithlag.show()
//    groupdata.show()

// 9th question  (not got)
//    val employee_attendance = Seq(("A001","E001","2023-11-01","Present Sales","Day"),("A002","E002","2023-11-02","Absent HR","Night"),
//      ("A003","E003","2023-11-03","Present IT","Night"),("A004","E001","2023-11-04","Absent Sales","Night"),
//      ("A005","E002","2023-11-05","Present HR","Day"))
//      .toDF("attendance_id","employee_id","attendance_date","status","department shift")
//    val attendance = employee_attendance.withColumn("attendance_date", to_date(col("attendance_date"),"yyyy-MM-dd")).withColumn("attendance_month", month(col("attendance_date")))
//    val filterdata = attendance.filter(col("status") ==="Absent" && col("shift") ==="Night")
//    val totalabsentdayDF = filterdata.groupBy("department","attendance_month").agg(count("Absent").as("Total_absent_days"))
//    val absentDaysperEmployeeDF = filterdata.groupBy("employee_id","department","attendance_month").agg(count("Absent").as("absent_days"))
//    val avgAbsentDaysDF = filterdata.groupBy("department","attendance_month").agg(avg("Absent_days").as("Avg_Absent_days_per_employee"))
//    val maxStreakDF = filterdata.groupBy("department","attendance_month").agg(max("Absent_Streak").as("max_continuous_absence_streak"))
//    val windows = Window.partitionBy("employee_id").orderBy("attendance_date")
//    val predictlead = employee_attendance.withColumn("next_attendance_status",lead("status", 1).over(windows))
//    totalabsentdayDF.show()
//    absentDaysperEmployeeDF.show()
//    avgAbsentDaysDF.show()
//    maxStreakDF.show()
//    predictlead.show()

    // 10th question
//    val flight_delays = Seq(
//  ("F001", "Delta", "2023-11-01 08:00", "2023-11-01 10:00", 40, "New York"),
//  ("F002", "United", "2023-11-01 09:00", "2023-11-01 11:30", 20, "New Orleans"),
//  ("F003", "American", "2023-11-02 07:30", "2023-11-02 09:00", 60, "New York"),
//  ("F004", "Delta", "2023-11-02 10:00", "2023-11-02 12:15", 30, "Chicago"),
//  ("F005", "United", "2023-11-03 08:45", "2023-11-03 11:00", 50, "New York"))
//      .toDF("flight_id", "airline", "departure_time", "arrival_time", "delay", "destination")
//    // Convert columns to timestamp
//     val flight_delays_with_time = flight_delays
//     .withColumn("departure_time", to_timestamp($"departure_time","yyyy-MM-dd HH:mm")).withColumn("arrival_time", to_timestamp($"arrival_time","yyyy-MM-dd HH:mm"))
//    val flight_delays_with_delay_minutes = flight_delays_with_time
//    .withColumn("delay_minutes", unix_timestamp($"arrival_time") - unix_timestamp($"departure_time") / 60)
//    val filtered_df = flight_delays_with_delay_minutes
//    .filter(col("delay_minutes") > 30 && col("destination").startsWith("New"))
//      val grouped_df = filtered_df.groupBy("airline", "destination").agg(
//    sum("delay_minutes").as("total_delay_minutes"),
//    max("delay_minutes").as("max_delay"),
//    avg("delay_minutes").as("avg_delay"))
//    val windowSpec = Window.partitionBy("airline").orderBy("departure_time")
//    val flight_delays_with_lag = flight_delays_with_delay_minutes
//   .withColumn("previous_delay", lag("delay_minutes", 1).over(windowSpec))
//    flight_delays_with_lag.show()
//    grouped_df.show()

  // 11th question
//    val customer_purchases = Seq(
//  ("P001", "C001", "Running Shoes", "2023-10-10", 100, "Credit Card"),
//  ("P002", "C002", "Basketball Shoes", "2023-11-01", 150, "Debit Card"),
//  ("P003", "C003", "Soccer Shoes", "2023-12-01", 120, "Credit Card"),
//  ("P004", "C001", "Dress Shoes", "2024-01-15", 200, "Credit Card"),
//  ("P005", "C004", "Running Shoes", "2024-02-10", 110, "Cash")
//  ).toDF("purchase_id", "customer_id", "product_name", "purchase_date", "purchase_amount", "payment_method")
//   val purchasedata = customer_purchases.withColumn("purchase_date", to_date(col("purchase_date"))).withColumn("purchase_month", month(col("purchase_date")))
//   val filterdata = purchasedata.filter(col("product_name").endsWith("Shoes") && col("payment_method").startsWith("Credit"))
//    val groupdata = filterdata.groupBy("customer_id","purchase_month").agg(
//      sum("purchase_amount").as("total_purchase_amount"),
//      min("purchase_amount").as("minimum_purchase_amount"),
//      count("purchase_id").as("purchase_count"))
//    val windows = Window.partitionBy("customer_id").orderBy("purchase_date")
//    val purchase_with_lead = purchasedata.withColumn("next_purchase_amount", lead("purchase_amount", 1).over(windows))
//      .withColumn("purchase_amount_difference", col("next_purchase_amount") -col("purchase_amount"))
//      purchase_with_lead.show()
//      groupdata.show()

    //12th question
//    val product_stock = List(("P001","Mobile Phone","Electronics","2023-10-01",80,"Supplier A"),
//    ("P002","Washing Machine","Home Appliances","2023-11-05",120,"Supplier B"),
//      ("P003","Laptop","Electronics","2023-12-10",60,"Supplier A"),
//        ("P004","TV","Consumer Electronics","2023-12-15",95,"Supplier C"),("P005","Mobile Phone","Electronics","2024-01-01",40,"Supplier A"))
//      .toDF("product_id","product_name","category","stock_date","stock_level","supplier")
//    val stockdata = product_stock.withColumn("stock_month", month(col("stock_date")))
//    val filterdata = stockdata.filter(col("category").startsWith("Electro") && col("stock_level") < 100)
//    val groupdata = filterdata.groupBy("supplier","stock_month").agg(
//      avg("stock_level").as("avg_stock_level"),
//      max("stock_level").as("max_stock_level"),
//      sum(when(col("stock_level") < 50,1).otherwise(0)).as("low_stock_count"))
//     val windata = Window.partitionBy("product_id").orderBy("stock_level")
//     val datalag = filterdata.withColumn("previous_stock",lag("stock_level", 1).over(windata))
//     datalag.show()
//    groupdata.show()

    //13th question
//    val bank_transactions = List(("T001","A001","Withdrawal","2023-11-01",6000,"Branch X"),
//      ("T002","A002","Deposit","2023-11-05",2000,"Branch Y"),("T003","A003","Withdrawal","2023-12-01",5500,"Branch X"),
//      ("T004","A004","Withdrawal","2023-12-10",7000,"Branch Z"),("T005","A005","Deposit","2024-01-15",3000,"Branch Y"))
//      .toDF("transaction_id","account_id","transaction_type","transaction_date","transaction_amount","branch")
//    val transationdata = bank_transactions.withColumn("transaction_year", year(col("transaction_date")))
//    val filterdata = transationdata.filter(col("transaction_type").startsWith("Withdrawal") && col("transaction_amount") > 5000)
//    val groupdata = filterdata.groupBy("branch","transaction_year").agg(
//      sum("transaction_amount").as("Total_amount_withdrawals"),
//      avg("transaction_amount").as("Avg_amount_deposits"),
//      count("transaction_id").as("transaction_count"))
//    val windows = Window.partitionBy("account_id").orderBy("transaction_date")
//    val amountlag = filterdata.withColumn("previous_transaction_amount",lag("transaction_amount", 1).over(windows))
//    amountlag.show()

    //14th question
//    val employee_salaries = Seq(("E001","John", "IT",90000,"2023-11-01","Senior Developer"),("E002","Emma","HR",75000,"2023-12-01","HR Manager"),("E003","Liam","IT",85000,"2023-10-15","Senior Analyst"),
//    ("E004","Olivia", "Sales",60000, "2023-11-20", "Sales Executive"),("E005","Noah", "IT",95000,"2024-01-01","Senior Developer"))
//      .toDF("employee_id","name","department","salary","last_increment_date", "position")
//    val dfWithYear = employee_salaries.withColumn("increment_year", year(col("last_increment_date")))
//    val filteredDf = dfWithYear.filter(col("salary") > 80000 && col("position").startsWith("Senior"))
//    val groupedDf = filteredDf.groupBy("department", "increment_year").agg(
//    avg("salary").as("average_salary"),
//    max("salary").as("max_salary"),
//    count("employee_id").as("employee_count"))
//    groupedDf.show()
//    val windowSpec = Window.partitionBy("employee_id").orderBy("last_increment_date")
//    val dfWithNextIncrement = filteredDf.withColumn("next_increment_salary", lead("salary", 1).over(windowSpec))
//    dfWithNextIncrement.show()

   //15th question
//   val library_borrowing = List(("B001","M001","War and Peace","2023-10-01","2023-10-15","Classic Fiction"),
//    ("B002","M002","The Great Gatsby","2023-11-05","NULL","Modern Fiction"),
//      ("B003","M003","1984","2023-12-10","2023-12-20","Dystopian Fiction"),
//    ("B004","M001","To Kill a Mockingbird","2023-12-15","NULL","Modern Fiction"),
//    ("B005","M004","Moby Dick","2024-01-01","2024-01-20","Classic Fiction")
//   ).toDF("borrow_id","member_id","book_title","borrow_date","return_date","category")
//    val borrowdata = library_borrowing.withColumn("borrow_month", month(col("borrow_date")))
//    val filterdata = borrowdata.filter(col("category").endsWith("Fiction") && col("return_date").isNull)
//    val groupdata = filterdata.groupBy("category","borrow_month").agg(
//      count("borrow_id").as("total_books_borrowed"),
//      avg(datediff(col("borrow_date"), col("return_date")).as("avg_borrowing_duration")),
//      count(col("return_date")).as("count_books_not_returned"))
//    val data = Window.partitionBy("member_id").orderBy("borrow_date")
//    val lagd_f = borrowdata.withColumn("previous_borrow_date",lag("borrow_date", 1).over(data))
//    val timedata = lagd_f.withColumn("time_between_borrowing",datediff(col("borrow_date"), col("previous_borrow_date")))
//    timedata.show()

  // 16th question (no coming)
//   val course_completion = List(("C001","S001","Data Science","2023-11-01",90,"True"),
//      ("C002","S002","Web Development","2023-12-01",80,"False"),
//      ("C003","S003","Data Science","2023-10-15",85,"True"),
//      ("C004","S004","Machine Learning","2023-11-20" ,88,"True"),
//    ("C005","S005","Web Development","2024-01-01",92,"True")
//    ).toDF("completion_id","student_id","course_name","completion_date","score","certificate_awarded")
//    val dfWithYear = course_completion.withColumn("completion_year", year(col("completion_date")))
//    val filteredDf = dfWithYear.filter(col("score")>85 && col("certificate_awarded")===true)
//    val groupedDf = filteredDf.groupBy("course_name", "completion_year").agg(
//    count("student_id").as("total_students_completed"),
//    avg("score").as("average_score"),
//    sum(when(col("certificate_awarded"), 1).otherwise(0)).as("certificates_awarded"))
//    groupedDf.show()
//    val windowSpec = Window.partitionBy("student_id").orderBy("completion_date")
//    val dfWithLead = dfWithYear.withColumn("next_course", lead("course_name", 1).over(windowSpec))
//    dfWithLead.show()

   //17th question
//    val vehicle_maintenance = List(("M001","V001","2023-11-01",1500,"Repair","Garage A"),
//      ("M002","V002","2023-12-01",800,"Service","Garage B"),
//      ("M003","V001","2023-10-15",1200,"Repair","Garage A"),
//      ("M004","V003","2023-11-20",900,"Service","Garage C"),
//      ("M005","V002","2024-01-01",2000,"Repair","Garage B"))
//      .toDF("maintenance_id", "vehicle_id","maintenance_date","cost","service_type","garage")
//    val maindata = vehicle_maintenance.withColumn("maintenance_year", year(col("maintenance_date")))
//    val filterdata = maindata.filter(col("service_type").startsWith("Repair") && col("cost")>1000)
//    val groupdata = filterdata.groupBy("garage","maintenance_year").agg(
//      sum("cost").as("total_cost"),avg("cost").as("avg_cost"),
//      count(when(col("cost") > 1000 ,1)).as("high_cost_service_count"))
//      //count(when(col("cost"),1)).as("high_cost_service_count"))
//     groupdata.show()
//    val windows = Window.partitionBy("vehicle_id").orderBy("maintenance_date")
//    val leaddata = maindata.withColumn("previous_cost",lag("cost", 1).over(windows))
//    leaddata.show()

   // 18th question
//    val retail_discounts = List(("D001","P001","Leather Bag","2023-10-01",25,"Store A"),
//    ("D002","P002","Backpack","2023-11-05",15,"Store B"),
//      ("D003","P003","Handbag","2023-12-10",30,"Store A"),
//    ("D004","P004","Laptop Bag","2023-12-15",20,"Store C"),
//    ("D005","P005","Messenger Bag","2024-01-01",35,"Store A"))
//      .toDF("discount_id","product_id","product_name","discount_date","discount_percentage","store")
//    val discountdata = retail_discounts.withColumn("discount_month", month(col("discount_date")))
//    val filterdata = discountdata.filter(col("discount_percentage")>20 && col("product_name").endsWith("Bag"))
//    val groupdata = filterdata.groupBy("store","discount_month").agg(
//      avg("discount_percentage").as("avg_discount_percentage"),
//      max("discount_percentage").as("max_discount_percentage"),
//      count("discount_id").as("discount_count"))
//      groupdata.show()
//    val windowsp = Window.partitionBy("product_id").orderBy("discount_date")
//    val leaddata = discountdata.withColumn("next_discount_percentage",lead("discount_percentage", 1).over(windowsp))
//    leaddata.show()

  // 19th question
//    val student_performance = List(("S001","Mathematics","2023-10-01",45,"F","School A"),
//      ("S002","Science","2023-11-05",55,"D","School B"),
//      ("S003","History","2023-12-10",35,"F","School A"),
//      ("S004","Mathematics","2023-12-15",80,"B","School C"),
//      ("S005","English","2024-01-01",65,"C","School B"))
//      .toDF("student_id","subject","exam_date","score","grade", "school")
//    val dfWithMonth = student_performance.withColumn("exam_month", month(col("exam_date")))
//    val filteredDf = dfWithMonth.filter(col("score") < 50 && col("grade") === "F")
//    val groupedDf = filteredDf.groupBy("school", "exam_month").agg(
//    avg("score").as("average_score"),
//    max("score").as("highest_score_per_subject"),
//    count(when(col("grade") === "F", 1)).as("failing_grades_count"))
//    //count(when(col("grade"), 1)).as("failing_grades_count")
//    groupedDf.show()
//    val windowSpec = Window.partitionBy("student_id").orderBy("exam_date")
//    val dfWithLag = dfWithMonth.withColumn("previous_score", lag("score", 1).over(windowSpec))
//    val dfWithScoreChange = dfWithLag.withColumn("score_change", col("score") - col("previous_score"))
//    dfWithScoreChange.show()

    // 20th question (code is running but o/p is nill)
//    val restaurant_sales = List(("S001","R001","Pepperoni Pizza","2023-10-01",20,"Credit Card"),
//    ("S002","R002","Veggie Pizza","2023-11-05",15,"Cash"),
//      ("S003","R001","Cheese Pizza","2023-12-10",18,"Credit Card"),
//      ("S004","R003","BBQ Chicken Pizza","2023-12-15",22,"Debit Card"),
//      ("S005","R002","Margherita Pizza","2024-01-01",16,"Credit Card"))
//      .toDF("sale_id","restaurant_id","item_name","sale_date","sale_amount","payment_type")
//     val itemsdata = restaurant_sales.withColumn("sale_month", month(col("sale_date")))
//     val filterdata = itemsdata.filter(col("item_name").startsWith("Pizza") && col("payment_type")==="Credit Card")
//     val groupdata = filterdata.groupBy("restaurant_id","sale_month").agg(
//       sum("sale_amount").as("total_sale_amount"),
//       avg("sale_amount").as("Avg_sale_amount"),
//       count("payment_type").as("credit_card_count"))
//     groupdata.show()
//    val frames = Window.partitionBy("restaurant_id").orderBy("sale_month")
//    val leaddata = groupdata.withColumn("next_month_sale", lead("total_sale_amount", 1).over(frames))
//    leaddata.show()

// 21th question
//    val web_traffic = List(("S001","U001","https://www.shop.com","2024-02-01","12:30:00","Chrome",15),
//      ("S002","U002","https://www.news.com","2024-02-01","13:00:00","Firefox",30),
//        ("S003","U003","https://www.blog.com","2024-02-01","14:00:00","Safari",25),
//      ("S004","U001","https://www.shop.com","2024-02-02","12:45:00","Chrome",20),
//      ("S005","U004","https://www.news.com","2024-02-02","15:00:00","Firefox",35)
//     ).toDF("session_id","user_id","page_url","visit_date","visit_time","browser","session_duration")
//    val visitdata = web_traffic.withColumn("visit_day", dayofmonth(col("visit_date")))
//    val filterdata = visitdata.filter(col("page_url").startsWith("https://www.") && col("browser").isNotNull)
//    val groupdata = filterdata.groupBy("browser","visit_day").agg(
//      sum("session_duration").as("total_session_duration_per_day"),
//      max("session_duration").as("Maximum_session_duration_per_day"),
//      min("session_duration").as("Minimum_session_duration_per_day"),
//      count("visit_date").as("page_visit_count"))
//    val windows = Window.partitionBy("user_id").orderBy("visit_date","visit_time")
//    val leadd_f = filterdata.withColumn("next_session_duration",lead("session_duration", 1).over(windows))
//      .withColumn("session_duration_drop", when(col("session_duration") > col("next_session_duration"), "yes").otherwise("No"))
//    groupdata.show()
//    leadd_f.show()

   // 22th question
//    val ecommerce_orders = Seq(("O001","C001","Home Electronics","2024-01-01","2024-01-05",500,"Completed"),
//      ("O002","C002","Kitchen Electronics","2024-01-02","2024-01-06",200,"Completed"),
//      ("O003","C001","Home Electronics","2024-01-03","2024-01-07",600,"Pending"),
//      ("O004","C003","Personal Electronics","2024-01-04","2024-01-08",150,"Completed"),
//      ("O005","C002","Kitchen Electronics","2024-01-05","2024-01-09",300,"Completed"))
//      .toDF("order_id","customer_id","product_category","order_date","delivery_date","order_value","payment_status")
//    val deliverydata = ecommerce_orders.withColumn("delivery_time", datediff(col("order_date"),col("delivery_date")))
//    val filterdata = deliverydata.filter(col("product_category").endsWith("Electronics") && col("payment_status") === "Completed")
//    val groupdata = filterdata.groupBy("customer_id","product_category").agg(
//      sum("order_value").as("total_order_value"),
//      avg("delivery_time").as("Average_delivery_time"),
//      count("order_id").as("count_orders"))
//     groupdata.show()
//    val data = Window.partitionBy("customer_id").orderBy("order_date")
//    val lagd_f = filterdata.withColumn("previous_delivery_time",lag("delivery_time", 1).over(data))
//      .withColumn("delivery_time_change",when(col("delivery_time") > col("previous_delivery_time"), "Increased")
//        .when(col("delivery_time") < col("previous_delivery_time"), "Decreased"))
//     lagd_f.show()

   //23th question
//    val social_media_engagement = Seq(("P001","U001","Video","2024-03-01",150,50,20),
//      ("P002","U002","Image","2024-03-01",100,25,15),
//      ("P003","U003","Video","2024-03-02",200,75,30),
//      ("P004","U001","Video","2024-03-02",180,60,25),
//      ("P005","U004","Article","2024-03-03",90,20,10)
//     ).toDF("post_id","user_id","post_type","post_date","like_count","share_count","comment_count")
//    val scoredata = social_media_engagement.withColumn("engagement_score", col("like_count")* 0.5 + col("share_count")* 1.0 + col("comment_count")* 1.5)
//    val filterdata = scoredata.filter(col("post_type").startsWith("Video") && col("engagement_score") > 100)
//    val groupdata = filterdata.groupBy("user_id","post_type").agg(
//      sum("engagement_score").as("total_engagement_score"),
//      max("like_count").as("max_like_count"),
//      max("comment_count").as("max_comment_count"),
//      count("post_id").as("post_count"))
//    val window = Window.partitionBy("user_id").orderBy("post_date")
//    val leadd_f = scoredata.withColumn("next_engagement_score",lead("engagement_score", 1).over(window))
//      .withColumn("engagement_score_change", when(col("engagement_score") > col("next_engagement_score"),"Increased")
//      .when(col("engagement_score") < col("next_engagement_score"),"Decreased").otherwise("No_changes"))
//      leadd_f.show()

    //24th question
//    val inventory_turnover = List(("I001","W001","Widget A","2024-01-01","2024-01-20",100,120),
//    ("I002", "W002","Widget B","2024-01-02","2024-02-01",200,180),
//      ("I003","W001","Widget A","2024-01-15","2024-02-05",150,160),
//    ("I004","W003","Widget C","2024-02-01","2024-02-15",80,90),
//    ("I005","W002","Widget B","2024-02-10","2024-02-25",200,220))
//      .toDF("inventory_id","warehouse_id", "product_name", "stock_in_date", "stock_out_date", "stock_in_quantity", "stock_out_quantity")
//    val trundata = inventory_turnover.withColumn("turnover_duration",datediff(col("stock_in_date"), col("stock_out_date")))
//    val filterdata = trundata.filter(col("turnover_duration") < 30 && col("stock_out_quantity") > "stock_in_quantity")
//    val groupdata = filterdata.groupBy("warehouse_id","product_name").agg(
//      avg("turnover_duration").as("avg_turnover_duration"),
//      sum("stock_out_quantity").as("total_stock_out_quantity"),
//      count("inventory_id").as("turnover_cycle"))
//    val window = Window.partitionBy("product_name").orderBy("stock_in_date")
//    val lagdata = trundata.withColumn("previous_stock_in_quantity",lag("stock_in_quantity", 1).over(window))
//      .withColumn("quality_change", when(col("stock_in_quantity") > col("previous_stock_in_quantity"),"Increased")
//      .when(col("stock_in_quantity") < col("previous_stock_in_quantity"),"Decreases").otherwise("N0_changes"))
//       lagdata.show()

    //25th question
//    val data = Seq(
//  ("C001", "CU001", "Premium", "2023-01-01", "2024-01-01", 120, true),
//  ("C002", "CU002", "Standard", "2023-02-01", "2024-02-01", 80, false),
//  ("C003", "CU003", "Premium", "2023-03-01", "2024-03-01", 150, true),
//  ("C004", "CU004", "Basic", "2023-04-01", "2024-04-01", 50, false))
//  .toDF("churn_id", "customer_id", "plan_type", "signup_date", "last_activity_date", "monthly_spend", "churn_flag")
//   val customerdata = data.withColumn("customer_lifetime", datediff(col("signup_date"), col("last_activity_date")))
//   val filterdata =  customerdata.filter(col("churn_flag") === "True" && col("monthly_spend") > 100)
//    val groupdata = filterdata.groupBy("plan_type","customer_lifetime").agg(
//      sum("monthly_spend").as("total_monthly_spend"),
//      avg("customer_lifetime").as("Avg_customer_lifetime"),
//      count("churn_id").as("churned_customer_id"))
//    val window = Window.partitionBy("customer_id").orderBy("signup_date")
//    val leaddata = customerdata.withColumn("next_monthly_spend",lead("monthly_spend", 1).over(window))
//      .withColumn("spend_change", when(col("monthly_spend") > col("next_monthly_spend"),"Increased")
//      .when(col("monthly_spend") < col("next_monthly_spend"), "Decreased").otherwise("No change"))
//     groupdata.show()
//     leaddata.show()

    // Document
    // 1st question
//    var employees = List((1,"john",28),(2,"Jane",35),(3,"Doe",35)).toDF("id","name","age")
//    var newDF = employees.select(col("id"),col("name"),col("age"),when(col("age")>=18,"true").otherwise("false")).as("is_adult")
//    newDF.show()

    //2nd question
//    var grades = List((1,85),(2,42),(3,73)).toDF("student_id","score")
//    var score = grades.select(col("student_id"),col("score"),when(col("score")>=50,"pass").otherwise("Fail")).alias("grade")
//    score.show()

    //3rd question
//    var transactions = List((1,1000),(2,200),(3,5000)).toDF("transaction_id","amount")
//    var categorydf = transactions.select(col("transaction_id"),col("amount"),when(col("amount")>1000,"High"),
//      when(col("amount")===500 && col("amount") ===1000,"Medium").otherwise("Low")).as("category")
//     categorydf.show()

    //4th question
//    var products = List((1,30.5),(2,150.75),(3,75.25)).toDF("product_id","price")
//    var pricedf = products.select(col("product_id"),col("price"),when(col("price")<50,"Cheap"),when(col("price") ===50 && col("price") ===100,"Moderate").otherwise("Expensive"))
//      .as("price_range")
//     pricedf.show()

   //5th question
//    var events = List((1,"2024-07-27"),(2,"2024-12-25"),(3,"2025-01-01")).toDF("event_id","date")
//    var holiday = events.select(col("event_id"),col("date"),when(col("date") ==="2024-12-25" || col("date") === "2025-01-01","true")
//    .otherwise("false").as("is_holiday")).show()

    //1th question  (Medium Questions)
//    var inventory = List((1,5),(2,15),(3,25)).toDF("item_id","quantity")
//    var stock = inventory.select(col("item_id"),col("quantity"),when(col("quantity")<10,"Low")
//    ,when(col("quantity")===10 && col("quantity")===20,"Medium").otherwise("High")).as("stock_level")
//    stock.show()

    //2nd question
//    var customer = List((1,"john@gmail.com"),(2,"jane@yahoo.com"),(3,"doe@hotmail.com")).toDF("customer_id","email")
//    var email = customer.withColumn("email_provider",when(col("email").contains("gmail"),"Gmail").when(col("email").contains("yahoo"),"Yahoo").otherwise("Other"))
//    email.show()

    //3rd question
//    var orders = List((1,"2024-07-01"),(2,"2024-12-01"),(3,"2024-05-01")).toDF("order_id","order_date")
//    var season = orders.select(col("order_id"),col("order_date"),when(month(col("order_date")).isin(6,7,8),"summer")
//    ,when(month(col("order_date")).isin(12,1,2),"Winter").otherwise("Other")).as("season")
//    season.show()

    //4th question
//    var sales = List((1,100),(2,1500),(3,300)).toDF("sale_id","amount")
//    var discount = sales.select(col("sale_id"),col("amount"),when(col("amount")<200,"0"),when(col("amount")===200 && col("amount")===1000,"10")
//    ,when(col("amount")<1000,"20")).as("discount")
//    discount.show()

   // 5th question
//    var logins = List((1,"09:00"),(2,"18:30"),(3,"14:00")).toDF("login_id","login_time")
//    var morning = logins.select(col("login_id"),col("login_time"),when(col("login_time")))

    //complex questions
    //1st question
//    var employees = List((1,25,30000),(2,45,50000),(3,35,40000)).toDF("employee_id","age","salary")
//    var category = employees.select(col("employee_id"),col("age"),col("salary"),when(col("age")<30 && col("salary")<35000,"Young & LowSalary")
//      ,when(col("age")===30 && col("age")===40 && col("salary")===35000 && col("salary")===45000,"MiddleAged & Medium Salary").otherwise("Old & High salary"))
//    .alias("category").show()

   //2nd question
//    var reviews = List((1,1),(2,4),(3,5)).toDF("review_id","rating")
//    var feedback = reviews.select(col("review_id"),col("rating").alias("feedback"),when(col("rating")<3,"Bad"),when(col("rating")===3 || col("rating")===4,"Good")
//      ,when(col("rating")===5,"Excellent"),when(col("rating")>=3,"is_positive").otherwise("false")).show()

   //3rd question
//    var document = List((1,"The quick brown fox"),(2,"Lorern ipsum dolor sit amet"),(3,"Spark is a unified analytics engine")).toDF("doc_id","content")
//    var content = document.select(col("doc_id"),col("content"),
//      when(col("content").contains("fox"),"Animal Related",when(col("content").contains("Lorem"),"placeholder" && col("content").contains("Spark")),"Tech Related")
//    .alias("content_category")).show()

   // 4th question
    //var tasks = List((1,"2024-07-01","2024-07-10"),(2,"2024-08-01","2024-08-15"),(3,"2024-09-01","2024-09-05")).toDF("task_id", "start_date", "end_date")
    //var taskdf = tasks.select(col("task_id"),col("start_date"),col("end_date"),when(col("start_date") && col("end_date")>7,"short"),when(col("")))

    // 1st question
    import org.apache.spark.sql.functions._

//    val data = Seq(("U001", "john.doe@gmail.com", "2024-01-01", "2024-03-01"),
//      ("U002", "jane.smith@outlook.com", "2024-02-15", "2024-03-10"),
//      ("U003", "alice.jones@yahoo.org", "2024-03-01", "2024-03-20"))
//          .toDF("user_id", "email", "signup_date", "last_login")
//        val domainDf = data.select(col("user_id"), col("email"), col("signup_date"), col("last_login"),
//       upper(regexp_extract(col("email"), "@(.+)$", 1)).alias("domain"))
//        val dateDf = domainDf.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
//       .withColumn("last_login", to_date(col("last_login"), "yyyy-MM-dd"))
//        val filteredDf = dateDf.filter(col("email").endsWith(".org"))
//       val uniqueUsersDf = filteredDf.dropDuplicates("user_id")
//       val resultDf = uniqueUsersDf.groupBy("domain").agg(count("user_id").alias("user_count"),
//        avg(datediff(col("last_login"),col("signup_date"))).as("avg_days_between_signup_login"))
//        resultDf.show()


   // 2nd question (not done)

//    val phone_date = Seq(("C001","+1-415-5551234","2024-01-10","WEST"), ("C002","+44-20-79461234","2024-02-20","EAST"),
//      ("C003","+91-22-23451234", "2024-03-15","NORTH")).toDF("customer_id","phone_number","signup_date","region")
//    val codedf = phone_date.select(col("customer_id"),col("phone_number"),col("signup_date"),col("region"),
//      regexp_extract(col("phone_number"), "([0-9]+)", 1).as("country_code"),
//      regexp_extract(col("phone_number"), "-([0-9]+)-", 1).as("area_code"),
//      regexp_extract(col("phone_number"), "-([0-9]+)$", 1).as("local_number"))
//    val convertdf = codedf.select(lower(col("region")).as("lower"))
//    val filterdf = convertdf.filter(col("country_code")=!="+1")
//    val dropdf = filterdf.dropDuplicates("phone_number")
//    val groupdf = dropdf.groupBy("region").agg(count("customer-id").as("customer_count"),count("area_count").as("count_area_count"))
//    val freqdf = groupdf.groupBy("region").agg(max("count_area_count")).as("frequent_count")
//    groupdf.show()
//    freqdf.show()

    ////// 2nd one
//    val phone_date = Seq(("C001","+1-415-5551234","2024-01-10","WEST"), ("C002","+44-20-79461234","2024-02-20","EAST"),
//      ("C003","+91-22-23451234", "2024-03-15","NORTH")).toDF("customer_id","phone_number","signup_date","region")
//    val codedf  = phone_date.withColumn("country_code",regexp_extract(col("phone_number"),"([0-9]+)", 1))
//      .withColumn("area_code",regexp_extract(col("phone_number"),("-([0-9]+)-"), 1))
//      .withColumn("local_number",regexp_extract(col("phone_number"),("-([0-9]+)$"),1))
//    val groupdf = codedf.groupBy("region").agg(count("customer_id").as("cust_count"),count("area_code").as("count_area_code"))
//    val freqcodedf = groupdf.groupBy("region").agg(max("count_area_code").as("freq_code"))
//    freqcodedf.show()

  // 3rd question

//    val product_data = Seq(("P001","A123-456-789","2024-04-01","electronics"),
//      ("P002","B234-567-891","2024-05-10","home_appliance"),
//      ("P003","X345-678-999","2024-06-15","furniture")).toDF("product_id","product_code","release_date","category")
//    val result = product_data.select(col("product_id"),col("product_code"),col("release_date"),col("category"),
//      split(col("product_code"), "-").getItem(0).as("brand_code"),
//      split(col("product_code"), "-").getItem(1).as("category_code"),
//      split(col("product_code"), "-").getItem(2).as("serial_number"),
//      initcap(col("category")).as("category"))
//    val filterdf = result.filter(col("brand_code").startsWith("X") && col("serial_number").endsWith("99"))
//    val dropdf = filterdf.dropDuplicates("product_code")
//    val groupdf = dropdf.groupBy("category_code").agg(sum(col("serial_number")).as("sum_serial_numbers"),
//      countDistinct(col("brand_code")).as("count_distinct_brands"))
//    groupdf.show()



    // 4th question
//    var web_logs = Seq(("S001","https://example.com/home","2024-07-01 10:00:00","Chrome/90.0"), ("S002","http://sample.org/contact","2024-07-02 11:30:00","Firefox/85.0"),
//      ("S003","https://example.com/admin", "2024-07-03 12:45:00", "Safari/14.1"))
//      .toDF("session_id", "url", "timestamp", "user_agent")
//    var extract = web_logs.select(col("session_id"),col("url"),col("timestamp"),col("user_agent")
//    ,lower(col("user_agent")).as("user_agent"),split(col("url"), "://").getItem(0).as("protocol"),
//    split(split(col("url"), "://").getItem(1), "/").getItem(0).as("domain"),
//    concat_ws("/", split(split(col("url"), "://").getItem(1), "/").getItem(1)).as("path")).show()
//    val filter = extract.filter(!col("path").startsWith("/admin"))
//    val drop = filter.dropDuplicates("session_id")
//    val group = drop.groupBy("domain").agg(avg(length(col("path"))).as("avg_path_length")
//      ,countDistinct("session_id").as("session_count"))
//    group.show()

    // 5th question
//    var address_data = Seq(("A001","123 Main St Apt 4B","New York","NY",10001),
//      ("A002","456 Elm St","San Francisco","CA",94102),("A003","789 Oak St Apt 12C","Chicago","IL",60603))
//      .toDF("address_id","full_address","city","state","zipcode")
//    var extract = address_data.select(col("address_id"),col("full_address"),col("city"),col("state"),col("zipcode")
//    ,upper(col("state")).as("state"),regexp_extract(col("full_address"),"^\\d+",0))

    // 6th question
//  val data = Seq(("I001", "T-shirt Red Large", 10, 20, 200),
//  ("I002", "Jeans Blue Medium", 3, 50, 150),
//  ("I003", "Jacket Black Small", 5, 100, 500))
//  val invoiceData = spark.createDataFrame(data).toDF("invoice_id", "description", "quantity", "unit_price", "total_amount")
//  val invoiceDataProcessed = invoiceData.select(
//    col("invoice_id"),col("quantity"),col("unit_price"),col("total_amount"),
//    upper(split(col("description"), " ").getItem(0)).as("product_name"),
//    split(col("description"), " ").getItem(1).as("color"),
//    split(col("description"), " ").getItem(2).as("size"))
//  val filteredData = invoiceDataProcessed
//  .select(col("invoice_id"),col("product_name"),col("color"),col("size"),
//    col("quantity"),col("unit_price"),col("total_amount"))
//  .filter(col("quantity") >= 5 && col("total_amount") <= 1000)
//  val groupedData = filteredData
//   .groupBy("color", "size")
//  .agg(sum("quantity").as("total_quantity"),avg("unit_price").as("average_unit_price"))
//  val finalData = filteredData
//  .dropDuplicates("product_name")  // Drop duplicates based on 'product_name' to mimic the effect of 'description'
//   groupedData.show()
//    finalData.show()

   //7th question
//    val data = Seq(("ORD001","P123-4567","2024-08-01","Delivered"),
//      ("ORD002","P234-5678","2024-08-05","Pending"),("ORD003","P345-6789","2024-08-10","Delivered"))
//    val orderData = spark.createDataFrame(data).toDF("order_id","product_code","order_date","delivery_status")
//    val extraDataprocess = orderData.select(col("order_id"),col("order_date"),col("product_code"),col("delivery_status")
//      substr(col("order_id"), 1, 3).as("order_id_prefix"),substr(col("product_code"),-4,4).as("product_code_suffix"))

  // 8th question
//    val data = List(("F001","AA123","2024-09-01","ON TIME"),
//      ("F002","DL456","2024-09-05","DELAYED"),("F003","UA789","2024-09-10","CANCELED"))
//    val positionData = spark.createDataFrame(data).toDF("flight_id","flight_number","departure_date","status")
//    val linedata = positionData.select(col("flight_id"),col("flight_number"), col("departure_date"),
//    upper(col("status")).as("status"),
//    // (assuming the airline code is the first 2 characters for the position)
//    instr(col("flight_number"), "AA").as("airline_code_pos"),
//    // Extract airline code using substr based on the position
//    when(instr(col("flight_number"), "AA") > 0, substr(col("flight_number"), 1, 2)).otherwise(null).as("airline_code"))
//   val filteredData = linedata.filter(col("status") === "ON TIME" && col("airline_code") === "AA")
//   val groupedData = filteredData.groupBy("airline_code").agg(
//    count("flight_number").as("flight_count"),
//    min("flight_number").as("min_flight_number"),
//    max("flight_number").as("max_flight_number"))
//   val finalData = positionData.dropDuplicates("flight_number")
//    groupedData.show()
//    finalData.show()

  // 9th question (regrex)
    //10th question
//    val data = Seq(("D001", "A quick brown fox jumps over the lazy dog", "John Doe", "2024-11-01"),
//     ("D002", "The quick brown fox", "Jane Smith", "2024-11-05"))
//    val splitDF = spark.createDataFrame(data).toDF("doc_id","content","author", "date")
//    val splitdata = splitDF.select(col("doc_id"),col("content"),col("author"),col("date")
//    ,initcap(col("author")).as("author"),explode(split(col("content")," ").as("word")))
//    val filterdata = splitdata.filter(!col("doc_id").startsWith("A") || col("doc_id").startsWith("An"))
//    val groupdata = filterdata.groupBy("author").agg(sum("word_count").as("total_word_count"),avg("word_count").as("avg_word_count"))
//    val dropdata = splitDF.dropDuplicates("content")
//    groupdata.show()
//    dropdata.show()

   //11th question regrex
    // 12th question regrex
//    val data = List(("C001","https://example.com/page?id=123&source=google","2024-12-05 11:45:00","GOOGLE"),
//   ("C002","https://sample.org/info?id=456&source=bing","2024-12-06 12:00:00","BING"),
//      ("C003","https://example.com/search?id=789&source=google","2024-12-07 13:30:00","YAHOO"))
//    val click_data = data.toDF("click_id","url","click_time","referrer")
//    val parameterdata = click_data.select(col("click_id"),col("url"),col("click_time"),col("referrer")
//    ,regexp_extract(col("url"), "\\?.*", 0)).as("query_params")
//    val upperdata = parameterdata.select(upper(col("referrer")))
//    val filterdata = upperdata.filter(col("query_params").contains("source=google")).show()


    // 13th question
//   val data = List(("F001","/docs/report1.pdf","2024-12-10",50),
//    ("F002","/images/picture1.jpg","2024-12-11",150),("F003","/docs/manual.docx","2024-12-12",75))
//    val file_data = data.toDF("file_id","file_path","upload_date","size_in_mb")
//    val file_data_with_extension = file_data.withColumn("file_name_ext", split(col("file_path"), "/").getItem(1))
//   .withColumn("extension", lower(split(col("file_path"), "/").getItem(2)))
//   val filtered_data = file_data_with_extension
//   .filter(col("extension") === "pdf" && col("size_in_mb") > 100)
//  val deduplicated_data = filtered_data.dropDuplicates(Seq("file_path"))
//  val result = deduplicated_data.groupBy("extension").agg(avg("size_in_mb").alias("avg_size_in_mb"),
//    countDistinct("file_name_ext").alias("distinct_file_count"))
//    result.show()

  // 17th question
//    val data = List(("C001","123 Main St.","new york","ny",10001),("C002","456 Elm Ave." ,"san francisco","ca",94102),("C003","789 Oak St.", "chicago","il",60603))
//    val  customerdata = data.toDF("customer_id","address","city","state","postal_code")
//    val standarddata = customerdata.select(col("customer_id"),col("address"),col("city"),col("state"),col("postal_code"),
//      regexp_replace(regexp_replace(regexp_replace(col("address"), "St\\.", "Street"), "Ave\\.", "Avenue"), "Blvd\\.", "Boulevard").as("standardized_address"))
//     val capital_data  = standarddata.select(initcap(col("city")).as("city"),initcap(col("state")).as("state"))
//    val filter = capital_data.filter(col("postal_code").isNull || col("postal_code") >=5 ).show()
//    val aggregatedData = standarddata.groupBy("state").agg(countDistinct("city").as("distinct_cities"),
//    count("customer_id").as("total_customers"))
//    val dropdata = customerdata.dropDuplicates("address")
//   aggregatedData.show(false)
//   dropdata.show()

  // 18th question
//    val data = List(("O001","ORD-001","2024-12-30",10,15,150),("O002","ORD@002","2024-12-31",5,20,100),("O003","ORD-003","2025-01-01",8,25,200))
//    val order_data = data.toDF("order_id","order_number","order_date","quantity","price_per_unit","total_price")
//    val specialdata = order_data.select(col("order_id"),col("order_number"),col("order_date"),col("quantity"),col("price_per_unit"),col("total_price"),
//    regexp_replace(regexp_replace(regexp_replace(col("order_number"), "-",""),"@",""),"-","")).as("cleaned_order_number")
//    val upperdata = specialdata.select(upper(col("order_number")))
//    val filterdata = upperdata.filter(col("total_price") ===col("quantity")* col("price_per_unit"))
//    val groupdata = specialdata.groupBy("order_date").agg(sum("quantity").as("total_quantity")
//    ,sum("total_price").as("total_revenue")).show()
//    val dropdata = order_data.dropDuplicates("order_number").show()

    //19th question
//    val data = List(("E001","JOHN DOE","Manager - Senior","Sales",75000),("E002","JANE SMITH","Analyst - Junior","IT",45000))
//    val emp_data = data.toDF("employee_id","name","role","department","salary")
//    val roledata = emp_data.select(col("employee_id"),col("name"),col("role"),col("department"),col("salary")
//    ,split(col("role"),"-").getItem(0).as("title"),split(col("role"),"-").getItem(1).as("level"))
//    val catalockdata = roledata.select(initcap(col("name")).as("name"))
//    val filterdata = catalockdata.filter(col("employee_id") < 50000 && col("level") ==="junior")
//    val groupdata = roledata.groupBy("department").agg(avg("salary").as("avg_salary"),countDistinct("role").as("distinct_count"))
//    val dropdatta = emp_data.dropDuplicates("name")
//    groupdata.show()
//    dropdatta.show()

  // 20th question
//    val data = List(("S001",15,"2024-12-28 08:00:00","C"),("S002",65,"2024-12-28 09:00:00","F"), ("S003",8,"2024-12-28 10:00:00","celsius"))
//    val sensor_data = data.toDF("sensor_id","reading_value","timestamp","unit")
//    val standarddata = sensor_data.select(col("sensor_id"),col("reading_value"),col("timestamp"),col("unit")
//    ,regexp_replace(col("unit"),"C","Celsius"),regexp_replace(col("unit"),"F","Fahrenheit")).as("standardize")
//    val lowerdata = standarddata.select(lower(col("unit")))
//    val filterdata = lowerdata.filter(!(col("reading_value")< 10 && col("unit")==="celsius"))
//    val groupdata = standarddata.groupBy("unit").agg(avg("reading_value").as("avg_reading_value"),countDistinct("sensor_id").as("distinct_count"))
//    val dropdata = sensor_data.dropDuplicates("sensor_id")
//    groupdata.show()
//    dropdata.show()

   // 23th question
//    val data = List(("I001","ABC-1234-X",60,"electronics","2024-12-28"),("I002","DEF-5678-Y",40,"clothing","2024-12-29"),
//      ("I003","GHI-9012-Z",100,"gadgets","2024-12-30"))
//    val inventory = data.toDF("item_id","product_code","quantity","category","last_checked")
//    val separatedata = inventory.select(col("item_id"),col("product_code"),col("quantity"),col("category"),col("last_checked")
//    ,split(col("product_code"),"-").getItem(0).as("prefix"),split(col("product_code"),"-").getItem(1).as("code_number")
//    ,split(col("product_code"),"-").getItem(3)).as("suffix")
//    val lowerdata = separatedata.select(lower(col("category")).as("category"))
//    val filterdata = lowerdata.filter(col("quantity")<50 && col("product_code").endsWith("X"))
//    val groupdata = separatedata.groupBy("category").agg(sum("quantity").as("total_quantity"),countDistinct("product_code").as("distinct_count"))
//    val dropdata = inventory.dropDuplicates("product_code")
//    groupdata.show()
//    dropdata.show()

    //24th question
//    val data = List(("U001","+1-555-1234567","2024-12-28","PREMIUM"),("U002","+44-20-1234567","2024-12-29","BASIC"),("U003","+91-9876543210","2024-12-30","PREMIUM"))
//    val userd_f = data.toDF("user_id","phone_number","sign_up_date","subscription_type")
//    val extractdata = userd_f.select(col("user_id"),col("phone_number"),col("sign_up_date"),col("subscription_type")
//    ,regexp_extract(col("phone_number"),"([0-9]+)", 1).as("country_code"))
//    val upperdata = extractdata.select(upper(col("subscription_type")).as("upper"))
//    val filterdata = upperdata.filter(!(col("country_code")==="+1" && col("subscription_type")==="BASIC"))
//    val groupdata = extractdata.groupBy("country_code").agg(count("user_id").as("total_users")).show()


    // 25th question
//    val data = List(("L001","192.168.1.1","2024-12-28 08:00:00",150),("L002","10.0.0.1","2024-12-28 09:30:00",50),("L003","172.16.0.1","2024-12-28 10:45:00",200))
//    val network = data.toDF("log_id","ip_address","access_time","data_transferred_mb")
//    val extract = network.select(col("log_id"),col("ip_address"),col("access_time"),col("data_transferred_mb")
//    ,split(col("ip_address"),"\\.").getItem(0).as("network"),split(col("ip_address"),"\\.").getItem(1).as("host"))
//    val networkdata = extract.select(initcap(col("network")).as("network"))
//    val filterdf = networkdata.filter(col("data_transferred_mb") >= 100).filter(col("network").startsWith("192"))
//    val groupdata = extract.groupBy("network").agg(sum("data_transferred_mb").as("total_data_transferred_mb"),countDistinct("log_id").as("distinct_log_id_count"))
//    val dropdata = network.dropDuplicates("ip_address")
//     groupdata.show()
//     dropdata.show()

   // 26th question
//    val data = List(("C001","John Doe",35,"new york","ny"),("C002","Jane Smith",25,"los angeles","ca"),("C003","Alice Johnson",45,"chicago","il"))
//    val customer = data.toDF("customer_id","full_name","age","city","state")
//    val splitdf = customer.select(col("customer_id"),col("full_name"),col("age"),col("city"),col("state")
//    ,split(col("full_name")," ").getItem(0).as("first_name"),split(col("full_name")," ").getItem(1).as("last_name"))
//    val citydf = splitdf.select(lower(col("city")).as("city"))
//    val filterdf = citydf.filter(col("age") <30 && col("city").contains("new"))
//    val groupdata = splitdf.groupBy("state").agg(avg("age").as("average_age"),countDistinct("city").as("distinct_city_count")).show()
//    val dropdata = customer.dropDuplicates("full_name")
//    dropdata.show()

  // 27th question
//    val data = List(("O001","ORD-001","2024-12-30",10,15,150),("O002","ORD-002","2024-12-31",5,20,100),("O003","ORD-003","2025-01-01",8,25,200))
//    val sales = data.toDF("order_id","order_number","order_date","item_quantity","item_price","total_value")
//    val splitdf = sales.select(col("order_id"),col("order_number"),col("order_date"),col("item_quantity"),col("item_price"),col("total_value")
//    ,split(col("order_number"),"-").getItem(0).as("order_number_prefix"),split(col("order_number"),"-").getItem(1).as("order_number_suffix"))
//    val order = splitdf.select(upper(col("order_number_prefix")).as("order_number"))
//    val filterdata = order.filter(col("total_value") === (col("item_quantity") * col("item_price")))
//    val groupdata = splitdf.groupBy("order_date").agg(sum("item_quantity").as("total_item_quantity"),sum("total_value").as("total_revenue"))
//    val dropdata = sales.dropDuplicates("order_number")
//    groupdata.show()
//    dropdata.show()

    // 29th question
//    val data = List(("P001","Special Widget A",45,"Gadgets","2024-11-15"),
//      ("P002","Standard Widget B",55,"Gadgets","2024-12-01"),("P003","Special Widget C",25,"Toys","2024-12-05"))
//    val product = data.toDF("product_id","product_name","price","category","release_date")
//    val splitdf = product.select(col("product_id"),col("product_name"),col("price"),col("category"),col("release_date")
//    ,split(col("product_name")," ").getItem(0).as("main_product"),split(col("product_name")," ").getItem(1).as("variant"))
//    val convert = splitdf.select(upper(col("product_name")).as("product_name"))
//    val filterdata = convert.filter(col("price") <50 && col("product_name").contains("SPECIAL")).show()
//    val groupdata = splitdf.groupBy("category").agg(avg("price").as("average_price"),countDistinct("product_name").as("distinct_count"))
//    val dropdata = product.dropDuplicates("product_id")
//    groupdata.show()
//    dropdata.show()

    // 30th question (reg-exp)
    // 32th question
//    val data = List(("T001",150,"2024-12-03","Main Store"),("T002",90,"2024-12-05","Secondary Store"),("T003",200,"2024-12-07","Main Store"))
//    val transaction = data.toDF("transaction_id","transaction_amount","transaction_date","store_location")
//    val datedf = transaction.select(col("transaction_id"),col("transaction_amount"),col("transaction_date"),col("store_location")
//    ,date_format(col("transaction_date"),"%Y").as("year"),date_format(col("transaction_date"),"%W").as("week"))
//    val strore = datedf.select(upper(col("store_location")).as("store_location"))
//    val filter = strore.filter(col("transaction_amount") >100 && col("store_location").endsWith("STORE"))
//    val groupdata = filter.groupBy("wee")


    /////
//    val inputSchema = "post_id int,content string,likes int,shares int,dte date"
//    val baseDF = spark.read.format("csv").option("header",true).schema(inputSchema)
//                    .csv("file_path")
//
//  val df2= baseDF.withColumn("hashtag",explode(split(regexp_extract(col("content"),"#.+",0)," ")))
//                 .withColumn("hashtag", lower(col("hashtag")))
//
//  //Hashtag Starts with ad and have 1000+ likes
//  df2.filter(col("hashtag").startsWith("#ad") && col("likes")>1000)
//     .select("hashtag")
//     .show()
//
//  //total likes and shares for each hashtag
//  df2.groupBy("hashtag").agg(sum(col("likes")).alias("total_likes")
//                            ,sum(col("shares")).alias("total_shares"))
//     .show()
//
//  //distinct postid for each hashtag
//  df2.groupBy("hashtag").agg(countDistinct(col("post_id")).alias("distinct_posts")).show()
//
//  //drop duplicates based on content
//  df2.dropDuplicates("content").show()

   //cap gemini
   //  list1 = ["a","b","c","d"] , list2 = [1,2,3,4]

   // ///
//    val locations = Array("/region=us/tablename=order_data/day=2024-06-01","/region=us/tablename=order_data/day=2024-06-02",
//      "/region=us/tablename=order_data/day=2024-06-03")
//    val dates = locations.map(_.split("/").last.split("=").last)
//    val region = dates.map(_.split("/").head.split("=").last).distinct.head
//    println("Dates: "+ dates.mkString(","))


   //val json1=spark.read.format("json").option("multiline",true).option("path","C:/Users/91957/Downloads/myjson1.json").load()
    //json1.select(col("id"),col("name"),col("age")).show()
//  val df=spark.read.format("json").option("multiline",true).option("path","C:/Users/91957/Downloads/myjson2.json").load()
//    df.show()
//  // Selecting and manipulating nested fields
//    val selectedDf = df.select(
//      col("id"),
//      col("name"),
//      col("address.city"),       // Access nested field 'city' from 'address'
//      col("address.zipcode"),    // Access nested field 'zipcode' from 'address'
//      explode(col("orders")).as("order") // Flatten 'orders' array
//    )
//    selectedDf.show(false)
//    val expandedDf = selectedDf.select(
//      col("id"),
//      col("name"),
//      col("city"),
//      col("zipcode"),
//      col("order.order_id"),    // Access nested field 'order_id' from exploded 'order'
//      col("order.amount")       // Access nested field 'amount' from exploded 'order'
//    )
//    expandedDf.show()

  //val data2 = List((1,"maths"),(1,"hindi"),(1,"english")).toDF("id","details")
  //data2.groupBy(col("id")).agg(collect_list(col("details")).as("detail")).show()


}


