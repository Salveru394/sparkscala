import org.apache.spark.sql.SparkSession

object problems {
def main(args: Array[String]): Unit = {

  val spark = SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()

  //val df=spark.read
    //.format("csv")
    //.option("header",true)
    //.schema(schema)
    //.option("path","C:/Users/91957/Documents/info.csv")
    //.load()


    //df.select(
    //col("id")
    //,col("Name")
    //,col("Age"),
    //when(col("age")<18,"young")
    //.when(col("age") >18 && col("age")<=30,"adult")
    //.otherwise("old")
    //.alias("details")
    //).show()

    //import spark.implicits._
    //var scoreData= Seq(("Alice", "Math", 80),("Bob", "Math", 90),
    //("Alice", "Science", 70),
    //("Bob", "Science", 85),
    //("Alice", "English", 75),
    //("Bob", "English", 95)
    //).toDF("Student", "Subject", "Score")
    //scoreData.groupBy(col("Student")).agg(avg(col("score")) , max(col("score"))).show()

    //var employees = List((1,25,30000),(2,45,50000),(3,35,40000)).toDF("employee_id","age","salary")
    //when(col("age")<30 && col("salary")<35000,"young & low salary")
    // .when(col("age") >=30 && col("age") <=40 && col("salary") >=35000 && col("salary") <=45000,"middle aged & medium salary")
    //.otherwise("old & high salary")

   //complex questions
    //9th question
    //df.select(col("payment_id"),col("payment_date"),
      //when(month(col("payment")).isin(1,2,3),"Q1")
        //.when(month(col("payment")).isin(4,5,6),"Q2")
        //.when(month(col("payment")).isin(7,8,9),"Q3")
        //.otherwise("Q4").alias("quarter")).show()

    //8th question
    //df.select(col("email_id"),col("email_address"),
     // when(col("email_address").contains("gmail"),"Gmail")
      //  .when(col("email_address").contains("yahoo"),"yahoo")
      //  .otherwise("Hotmail").alias("email_domain")).show()

    //7th question
    //df.select(col("student_id"),col("math_score"),col("english_score"),
      //when(col("math_score")> 80,"A").when(col("math_score").between(60,80),"B")
        //.otherwise("C").when(col("english_score") > 80,"A").when(col("english_score").between(60,80),"B")
        //.otherwise("C").alias("score")).show()

    //6th question
    //df.select(col("day_id"),col("temperature"),col("humidity"),
      //when(col("temperature") > 30,"is_hot").when(col("humidity") > 50,"is_humid")
        //.otherwise("false").alias("weather")).show()

    //5th question
    //df.select(col("order_id"),col("quantity"),col("total_price"),
     // when(col("quantity") < 10 && col("total_price") < 200,"small & cheap")
    //.when(col("quantity") <= 10 && col("total_price") < 200,"Bulk & Discounted")
    //.otherwise("premium").alias("order_type")).show()
    //4th question
    //df.select(col("task_id"),col("start_date"),col("end_date"),
      //when(datediff(col("start_date"),col("end_date")) < 7,"Short")
        //.when(datediff(col("start_date")).between(7, 14),"medium")
        //.otherwise("Long").alias("task_duration")).show()

    // 3rd question
    //df.select(col("doc_id"),col("content"),
      //when(col("content").contains("fox"), "Animal Related")
        //.when(col("content").contains("Lorem"), "Tech Related").otherwise("Spark")
        //.alias("content_category")).show()
    //2nd question
     //df.select(col("review_id"),col("rating"),
    //when(col("rating") < 3, "Bad").when(col("rating") >= 3 || col("rating") < 5, "Good").otherwise("Excellent")
    //.alias("feedback")).show()

    //1st question
    //df.select(col("employee_id"),col("age"),col("salary"),
    //when(col("age") < 30 && col("salary") < 35000,"young & low salary")
    //.when(col("age").between(30,40) && col("salary").between(35000,45000),"middle aged & medium salary").otherwise("Old & High Salary")
    //.alias("category")).show()

    // 5th question {medium}
    //df.select(col("login_id"),col("login_time"),
      //when(col("login_time") < "12:00", true).otherwise("false")
        //.alias("is_morning")).show()

    // 4th question {medium}
    //df.select(col("sale_id"),col("amount"),
    //when(col("amount") <200, 0).when(col("amount").between(200,1000), 10).otherwise(20)
    //.alias("discount")).show()

    //3rd question {medium}
    //df.select(col("order-id"),col("order-date"),
    //when(month(col("order-date")).isin(6,7,8), "summer").when(month(col("order-date")).isin(12,1,2), "winter").otherwise("other")
    //.alias("season")).show()

    //2 nd question {medium}
    //df.select(col("customer_id"), col("email"),
     //when(col("email").contains("gmail"),"Gmail").when(col("email").contains("yahoo"),"yahoo").otherwise("Other")
    //.alias("email_provider")).show()

    //1 th question {medium}
    //df.select(col("item_id"), col("quantity"),
     //when(col("quantity") < 10, "Low").when(col("quantity").between(10,20),"medium").otherwise("High")
    //.alias("stock_level")).show()

    // 5 th question
    //df.select(col("id"), col("date"),
     //when(col("date") === "2024-12-2025 || col("date") === "2025-01-01", true).otherwise(false)
       // .alias("is_holiday")).show()

    //4 th question
    //df.select(col("id"), col("price"),
    // when(col("price") < 50, "cheap").when(col("price")>=50 && col("price")<=100,"Moderate").otherwise("Expensive")
    //.alias("price_range")).show()

    //3 rd question
    //df.select(col("id"), col("amount"),
     //when(col("amount") > 1000, "high").when(col("amount")>=500 && col("amount")<=1000,"Medium").otherwise("Low")
    //.alias("category")).show()

    //2nd question
    //df.select(col("id"), col("score"),
     //when(col("score") >= 50, true).otherwise(false)
    //.alias("grade")).show()

    // 1st question
   // df.select(col("id"), col("name"), col("age"),
   //   when(col("age") >= 18, true).otherwise(false)
   //     .alias("is_adult")).show()

    //import spark.implicits._
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

    import spark.implicits._
    //var ratingsData= Seq(("user1", "Movie1", 4.5),("User2", "Movie1", 3.5),
    //("User3", "Movie2", 2.5),
    //("User4", "Movie2", 3.0),
    //("User1", "Movie3", 5.0),
    //("User2", "Movie3", 4.0)
    //).toDF("User", "Movie", "Rating")
    //ratingsData.groupBy(col("Movie")).agg( avg(col("Rating")) , count(col("Total_Rating")) ).show()
    //var averageRatings = ratingsData.groupBy("Movie").agg( avg("Rating").as("Average_rating"), count("Rating").as("Total_Ratings"))
    //averageRatings.show()


    //grade (questions)
    //val students =List((1,"Alice",92,"Math"), (2,"Bob",85,"Math"),
     //(3,"Carol",77,"Science"), (4,"Dave",65,"Science"), (5,"Eve",50,"Math"),
      //(6,"Frank",82,"Science")).toDF("student_id","name","score","subject")

    //val gradedf =students.withColumn("grades",when(col("score")>90,"A").when(col("score")<90 && col("score")>=80,"B")
      //.when(col("score")<80 && col("score")>=70,"C")
      //.when(col("score")<70 && col("score")>=60,"D")
      //.when(col("score")<60,"F"))
    //gradedf.show()

    //val groupdf =students.groupBy(col("subject"))
   //val avgdf =groupdf.agg(avg(col("score")))
    //val maxdf =groupdf.agg(max(col("score")))
    //val mindf =groupdf.agg(min(col("score")))
    //val countst =gradedf.groupBy(col("subject"),col("grades")).agg(count(col("grades")).alias("totalstudents"))
    //avgdf.show()
    //maxdf.show()
    //mindf.show()
    //countst.show()

    // 2nd question set = A
//    val products = List((1,"Smartphone",700,"Electronics"),(2,"TV",1200,"Electronics"),
//    (3,"Shoes",150,"Apparel"), (4,"Socks",25,"Apparel"),(5,"Laptop",800,"Electronics"),
//    (6,"Jacket",200,"Apparel")).toDF("product_id","product_name","price","category")
//
//  val prod1 = products.withColumn("price_category",when(col("price")>500,"Expensive")
//      .when(col("price")<=200 && col("price")<=500,"Moderate")
//    .otherwise("cheap")).show()
//
//    products.filter(col("product_name").startsWith("S")).show()
//    products.filter(col("product_name").endsWith("s")).show()
//    val prodgrp = prod1.groupBy(col("category"))
//    prodgrp.agg( sum(col("price").alias("total_price"))).show()
//    prodgrp.agg( avg(col("price").alias("average_price"))).show()
//    prodgrp.agg( min(col("price").alias("minimum_price"))).show()
//    prodgrp.agg( max(col("price").alias("maximum_price"))).show()

    // 3 rd question set = B
//    var employees = List((1,"John",28,60000),(2,"Jane",32,75000),(3,"Mike",45,120000),
//      (4,"Alice",55,90000),(5,"Steve",62,110000),(6,"Claire",40,40000)).toDF("employee_id","name","age","salary")
//
//     val emps1 = employees.withColumn("age_group",when(col("age")<30,"young").when(col("age")<=30 && col("age")<=50,"Mid")
//    .otherwise("senior")).withColumn("salary_range",when(col("salary")>100000,"High").when(col("salary")<=50000 && col("salary")<=100000,"Medium")
//      .otherwise("Low"))
//    emps1.show()
//
//    employees.filter(col("name").startsWith("j")).show()
//    employees.filter(col("name").endsWith("e")).show()
//    val empsgrp = emps1.groupBy(col("age_group"))
//    empsgrp.agg( sum(col("salary").alias("total_salary"))).show()
//    empsgrp.agg( avg(col("salary").alias("average_salary"))).show()
//    empsgrp.agg( max(col("salary").alias("maximum_salary"))).show()
//    empsgrp.agg( min(col("salary").alias("minimum_salary"))).show()

// 4th question
//val movies = List((1,"The Matrix",9,136),(2,"Inception",8,148),(3,"The Godfather",9,175),
//  (4,"Toy Story",7,81),(5,"TheShawshankRedemption",10,142),(6,"The Silence of the Lambs",8,118))
//  .toDF("movie_id","movie_name","rating","duration_minutes")
//
//    val mov1 = movies.withColumn("rating_category",when(col("rating")>=8,"Excellent").when(col("rating")<=6 && col("rating")<8,"Good")
//      .otherwise("Average")).withColumn("duration_category",when(col("duration_minutes")>150,"Long").when(col("duration_minutes")<=90 && col("duration_minutes")<=150,"Medium")
//    .otherwise("Short"))
//    mov1.show()
//
//    movies.filter(col("movie_name").startsWith("T")).show()
//    movies.filter(col("movie_name").endsWith("e")).show()
//    val movgrp = mov1.groupBy(col("rating_category"))
//    movgrp.agg( sum(col("duration_minutes").as("totaldurationminutes"))).show()
//    movgrp.agg( avg(col("duration_minutes").as("averagedurationminutes"))).show()
//    movgrp.agg( max(col("duration_minutes").as("maximumdurationminutes"))).show()
//    movgrp.agg( min(col("duration_minutes").as("minimumdurationminutes"))).show()

    // 5 th question (not done)
    // val transactions = List((1,"2023-12-01",1200,"Credit"),(2,"2023-11-15",600,"Debit"),(3,"2023-12-20",300,"Credit"),
   //(4,"2023-10-10",1500,"Debit"),(5,"2023-12-30",250,"Credit"),(6,"2023-09-25",700,"Debit"))
  //.toDF("transaction_id","transaction_date","date","amount","transaction_type")

   // val tran1 = transactions.withColumn("amount_category",when(col("amount")>1000,"High").when(col("amount")<=500 && col("amount")<1000,"Medium")
  //.otherwise("Low"))
   // tran1.show()
    //var transaction_month = transactions['transaction_date'].date.month_name()

// Set = B
//val feedback = List((1,"2024-01-10",4,"Great service!"),(2,"2024-01-15",5,"Excellent!"),
//(3,"2024-02-20",2,"Poor experience"),(4,"2024-02-25",3,"Good value"),(5,"2024-03-05",4,"Great quality"),
//(6,"2024-03-12",1,"Bad service")).toDF("customer_id","feedback_date","rating","feedback_text")

  //  val feeddf = feedback.withColumn("rating_category",when(col("rating")>=5,"Excellent").when(col("rating")<=3 && col("rating")<5,"Good")
   // .otherwise("poor"))
   // feeddf.show()
   // feedback.filter(col("feedback_text").startsWith("Great")).show()
   // val feedgrp = feeddf.groupBy("per month")
   // feedgrp.agg(month( avg("rating")).alias("average_rating_per_month")).show()

    // 2nd one (per month)
//val sales = List((1,"Widget",700,"2024-01-15"),(2,"Gadget",150,"2024-01-15"),(3,"Widget",350,"2024-02-15"),
    //(4,"Device",600,"2024-02-20"),(5,"Widget",100,"2024-03-05"),(6,"Gadget",500,"2024-03-12"))
  //.toDF("sale_id","product_name","sale_amount","sale_date")
    //val saledf = sales.withColumn("sale_category",when(col("sale_amount")>500,"High").when(col("sale_amount")<=200 && col("sale_amount")<=500,"Medium").otherwise("Low"))
    //saledf.show()
//   sales.filter(col("product_name").endsWith("t")).show()
//    val salegrp = saledf.groupBy(col("sale_date"))
//    salegrp.agg( sum("sale_amount")).as("Total sale_amount").show()
//    salegrp.agg( avg("sale_amount")).as("Average sale_amount").show()
//    salegrp.agg( max("sale_amount")).as("Maximum sale_amount").show()
//    salegrp.agg( min("sale_amount")).as("Minimum sale_amount").show()

    // 3 rd  one
//    val work = List((1,"2024-01-10",9,"Sales"),(2,"2024-01-11",7,"Support"),(3,"2024-01-12",8,"Sales"),
//      (4,"2024-01-13",10,"Marketing"),(5,"2024-01-14",5,"Sales"),(6,"2024-01-15",6,"Support"))
//      .toDF("employee_id","work_date","hours_worked","department")
//    val workdf = work.withColumn("hours_category",when(col("hours_worked")>8,"overtime").when(col("hours_worked")<=8,"Regular"))
//    workdf.show()
//    work.filter(col("hours_worked").startsWith("S")).show()
//    val workgrp = workdf.groupBy(col("department"))
//    workgrp.agg( sum("hours_worked")).as("Total hours_worked").show()
//    workgrp.agg( avg("hours_worked")).as("Average hours_worked").show()
//    workgrp.agg( max("hours_worked")).as("Maximum hours_worked").show()
//    workgrp.agg( min("hours_worked")).as("Minimum hours_worked").show()

    //4 th question
//    var inventory = List ((1,"Pro Widget",30,"2024-01-10"),(2,"Pro Device",120,"2024-01-15"),(3,"Standard",200,"2024-01-20"),
//(4,"Pro Gadget",40,"2024-02-01"),(5,"Standard",60,"2024-02-10")).toDF("product_id","product_name","stock","last_restocked")
//    var invendf = inventory.withColumn("stock_status",when(col("stock")<50,"Low").when(col("stock")<=50 && col("stock")<=150,"Medium").otherwise("High"))
//    invendf.show()
//    inventory.filter(col("product_name").contains("pro"))
//    var invengrp = invendf.groupBy(col("stock_status"))
//    invengrp.agg( sum("stock")).as("total_stock").show()
//    invengrp.agg( avg("stock")).as("Average_stock").show()
//    invengrp.agg( max("stock")).as("maximum_stock").show()
//    invengrp.agg( min("stock")).as("minimum_stock").show()

    // 5 th question
//    val transactions = List((1,1,1200,"2024-01-15"),(2,2,600,"2024-01-20"),(3,3,300,"2024-02-15"),(4,4,1500,"2024-02-20"),(5,5,200,"2024-03-05"),(6,6,900,"2024-03-12"))
//      .toDF("transaction_id","customer_id","transaction_amount","transaction_date")
//     val transfordf = transactions.withColumn("category",when(col("transaction_amount")>1000,"High").when(col("transaction_amount")<=500 && col("transaction_amount")<=1000,"Medium").otherwise("Low"))
//    transfordf.show()
//    /////transactions.filter(col("transaction_date").when("year") == "2024")
//    var transforgpp = transfordf.groupBy("category")
//    transforgpp.agg( sum("transaction_amount")).as("TotalAmount").show()
//    transforgpp.agg( avg("transaction_amount")).as("AverageAmount").show()
//    transforgpp.agg( max("transaction_amount")).as("maximumAmount").show()
//    transforgpp.agg( min("transaction_amount")).as("minimumAmount").show()

 // 6 th question
//    val performance_reviews = List((1,"2024-01-10",8,"Good performance"),(2,"2024-01-15",9,"Excellent work!"),(3,"2024-02-20",6,"Needs improvement"),(4,"2024-02-25",7,"Good effort"),
//   (5,"2024-03-05",10,"Outstanding!"),(6,"2024-03-12",5,"Needs improvement")).toDF("employee_id","review_date","performance_score","review_text")
//    val performdf = performance_reviews.withColumn("performance_category",when(col("performance_score")>=9,"Excellent").when(col("performance_score")<=7 && col("performance_score")<9,"Good")
//    .otherwise("Needs Improvement"))
//    performance_reviews.filter(col("review_text").contains("excellent"))
//    val performgrp = performdf.groupBy("review_date")
//    performgrp.agg( (avg("performance_score")).as("Average performance")).show()

// 7 th question
//    val product_review = List((1,"Smartphone",4,"2024-01-15"),(2,"Speaker",3,"2024-01-20"),(3,"Smartwatch",5,"2024-02-15"),(4,"Screen",2,"2024-02-20"),(5,"Speakers",4,"2024-03-05"),(6,"Soundbar",3,"2024-03-12"))
//      .toDF("review_id","product_name","rating","review_date")
//    val productdf = product_review.withColumn("rating_category",when(col("rating")>=4,"High").when(col("rating")<=3 && col("rating")<4,"Medium").otherwise("Low"))
//    productdf.show()
//    product_review.filter(col("product_name").startsWith("S"))
//    val productgrp = productdf.groupBy("rating_category")
//    productgrp.agg( count("review_id")).as("Total_Reviews").show()
//    productgrp.agg( avg("rating")).as("Average_Rating").show()

    //8th question
//    val sales_perform = List((1,"North-West",12000,"2024-01-10"),(2,"South-East",6000,"2024-01-15"),(3,"East-Central",4000,"2024-02-20"),(4,"West",15000,"2024-02-25"),(5,"North-East",3000,"2024-03-05"),(6,"South-West",7000,"2024-03-12"))
//      .toDF("sales_id","region","sales_amount","sales_date")
//    val sales_performdf = sales_perform.withColumn("sales_performance",when(col("sales_amount")>10000,"Excellent").when(col("sales_amount")<=500 && col("sales_amount")<=10000,"Good").otherwise("Average"))
//    sales_performdf.show()
//    sales_perform.filter(col("region").endsWith("West"))
//    val sales_performgrp = sales_performdf.groupBy("sales_performance")
//    sales_performgrp.agg ( sum("sales_amount")).as("Total_salesAmount").show()
//    sales_performgrp.agg ( avg("sales_amount")).as("Average_salesAmount").show()
//    sales_performgrp.agg ( max("sales_amount")).as("Maximum_salesAmount").show()
//    sales_performgrp.agg ( min("sales_amount")).as("Minimum_salesAmount").show()

    // 9th question
//    val purchase_history = List((1,1,2500,"2024-01-05"),(2,2,1500,"2024-01-15"),(3,3,500,"2024-02-20"),(4,4,2200,"2024-03-01"),(5,5,900,"2024-01-25"),(6,6,3000,"2024-03-12"))
//      .toDF("purchase_id","customer_id","purchase_amount","purchase_date")
//    val purchase_historydf = purchase_history.withColumn("purchase_category",when(col("purchase_amount")>2000,"Large").when(col("purchase_amount")<=1000 && col("purchase_amount")<=2000,"Medium").otherwise("Small"))
//    purchase_historydf.show()
//    ///purchase_history.filter(col("purchase_date"))
//    val purchase_historygrp = purchase_historydf.groupBy("purchase_category")
//    purchase_historygrp.agg ( sum("purchase_amount")).as("Total_purchase_amount").show()
//    purchase_historygrp.agg ( avg("purchase_amount")).as("Average_purchase_amount").show()
//    purchase_historygrp.agg ( max("purchase_amount")).as("maximum_purchase_amount").show()
//    purchase_historygrp.agg ( min("purchase_amount")).as("minimum_purchase_amount").show()

    // 10th question
//    val attendance = List((1,"2024-01-10",9,"Sick"),(2,"2024-01-11",7,"Scheduled"),(3,"2024-01-12",8,"Sick"),(4,"2024-01-13",4,"Scheduled"),(5,"2024-01-14",6,"Sick"),(6,"2024-01-15",8,"Scheduled"))
//      .toDF("employee_id","attendance_date","hours_worked","attendance_type")
//    val attendencedf = attendance.withColumn("attendance_status",when(col("hours_worked")>=8,"full Day").when(col("hours_worked")<8,"HalfDay"))
//    attendencedf.show()
//    attendance.filter(col("attendance_type").startsWith("S"))
//    val attendancegrp = attendencedf.groupBy("attendance_status")
//    attendancegrp.agg( sum("hours_worked")).as("Total_hours_worked").show()
//    attendancegrp.agg( avg("hours_worked")).as("average_hours_worked").show()
//    attendancegrp.agg( max("hours_worked")).as("maximum_hours_worked").show()
//    attendancegrp.agg( min("hours_worked")).as("minimum_hours_worked").show()

    // 11 th question
//    val book_inventory = List((1,"The Great Gatsby",150,"2024-01-10"),(2,"The Catcher in the Rye",80,"2024-01-15"),(3,"Moby Dick",200,"2024-01-20"),
//      (4,"To Kill a Mockingbird",30,"2024-02-01"),(5,"The Odyssey",60,"2024-02-10"),(6,"War and Peace",20,"2024-03-01"))
//      .toDF("book_id","book_title", "stock_quantity", "last_updated")
//    val book_inventorydf = book_inventory.withColumn("stock_level",when(col("stock_quantity")>100,"High").when(col("stock_quantity")<=50 && col("stock_quantity")<=100,"Medium").otherwise("Low"))
//    book_inventorydf.show()
//    book_inventory.filter(col("book_title").startsWith("The"))
//    val book_inventorygrp = book_inventorydf.groupBy("stock_level")
//    book_inventorygrp.agg( sum("stock_quantity")).as("Total_stock_quantity").show()
//    book_inventorygrp.agg( avg("stock_quantity")).as("Average_stock_quantity").show()
//    book_inventorygrp.agg( max("stock_quantity")).as("Maximum_stock_quantity").show()
//    book_inventorygrp.agg( min("stock_quantity")).as("Minimum_stock_quantity").show()

    // 12th question
//    val showtimes = List((1,"Action Hero","2024-01-10",8),(2,"Comedy Nights","2024-01-15",25), (3,"Action Packed","2024-01-20",55),(4,"Romance Special","2024-02-01",5),(5,"Action Force","2024-02-10",45),(6,"Drama Series","2024-03-01",70))
//      .toDF("show_id","movie_title","showtime","seats_available")
//    val showtimesdf = showtimes.withColumn("availability",when(col("seats_available")<=10,"Full").when(col("seats_available")<=11 && col("seats_available")<=50,"Limited").otherwise("Plenty"))
//    showtimesdf.show()
//    showtimes.filter(col("movie_title").contains("Action"))
//    val showtimegrp = showtimesdf.groupBy("availability")
//    showtimegrp.agg ( sum("seats_available")).as("Total_seats_available").show()
//    showtimegrp.agg ( avg("seats_available")).as("average_seats_available").show()
//    showtimegrp.agg ( max("seats_available")).as("maximum_seats_available").show()
//    showtimegrp.agg ( min("seats_available")).as("minimum_seats_available").show()

  //13 th question
//    val salaries = List((1,"IT",130000,"2024-01-10"),(2,"HR",80000,"2024-01-15"),(3,"IT",60000,"2024-02-20"),(4,"IT",70000,"2024-02-25"),(5,"Sales",50000,"2024-03-05"),(6,"IT",90000,"2024-03-12"))
//      .toDF("employee_id","department","salary","last_increment_date")
//    val salariesdf = salaries.withColumn("salary_band",when(col("salary")>120000,"High").when(col("salary")<=60000 && col("salary")<=12000,"Medium").otherwise("Low"))
//   salaries.filter(col("department").startsWith("IT"))
//    val salariesgrp = salariesdf.groupBy("salary_band")
//    salariesgrp.agg( sum("salary")).as("Total_salary").show()
//    salariesgrp.agg( avg("salary")).as("Average_salary").show()
//    salariesgrp.agg( max("salary")).as("Maximum_salary").show()
//    salariesgrp.agg( min("salary")).as("Minimum_salary").show()

    // 14th question
//    val shipments = List((1,"Asia",15000,"2024-01-10"),(2,"Europe",6000,"2024-01-15"),(3,"Asia",3000,"2024-02-20"),(4,"Asia",20000,"2024-02-25"),(5,"North America",4000,"2024-03-05"),(6,"Asia",8000,"2024-03-12"))
//      .toDF("shipment_id"," destination"," shipment_value","shipment_date")
//    val shipmentdf = shipments.withColumn("value_category",when(col("shipment_value")>10000,"High").when(col("shipment_value")<=5000 && col("shipment_value")<=10000,"Medium").when(col("shipment_value")<5000,"Low"))
//    shipmentdf.show()
//    shipments.filter(col("destination").contains("Asia"))
//    val shipmentsgrp = shipmentdf.groupBy("value_category")
//    shipmentsgrp.agg( sum("shipment_value")).as("total_shipment_value").show()
//    shipmentsgrp.agg( avg("shipment_value")).as("average_shipment_value").show()
//    shipmentsgrp.agg( max("shipment_value")).as("maximum_shipment_value").show()
//    shipmentsgrp.agg( min("shipment_value")).as("minimum_shipment_value").show()

    // 15th question
//    val online_purchase = List((1,1,700,"2024-02-05"),(2,2,150,"2024-02-10"),(3,3,400,"2024-02-15"),(4,4,600,"2024-02-20"))
//      .toDF("purchase_id","customer_id","purchase_amount","purchase_date")
//   val onlinedf = online_purchase.withColumn("purchase_status",when(col("purchase_amount")>500,"Large").when(col("purchase_amount")<=200 && col("purchase_amount")<=500,"Medium").otherwise("Small"))
//    onlinedf.show()
//    online_purchase.filter(col("purchase_date")> "February 2024")
//    val onlinegrp = onlinedf.groupBy("purchase_status")
//    onlinegrp.agg ( sum("purchase_amount")).as("total_purchase_amount").show()
//    onlinegrp.agg ( avg("purchase_amount")).as("average_purchase_amount").show()
//    onlinegrp.agg ( max("purchase_amount")).as("maximum_purchase_amount").show()
//    onlinegrp.agg ( min("purchase_amount")).as("minimum_purchase_amount").show()

    // 16th question
//    val sales_targets = List((1,"John Smith",15000,12000),(2,"Jane Doe",9000,10000),(3,"john Doe",5000,6000),(4,"John Smith",13000,13000),(5,"Jane Doe",7000,7000),(6,"John Doe",8000,8500))
//      .toDF("sales_id","sales_rep","sales_amount","target_amount")
//     val salesdf = sales_targets.withColumn("achievement_status",when(col("sales_amount")>="target_amount", "AboveTarget").when(col("sales_amount")< "target_amount","Below_Target"))
//       salesdf.show()
//    sales_targets.filter(col("sales_rep").contains("John"))
//    val salesgrp = salesdf.groupBy("achievement_status")
//    salesgrp.agg( sum("sales_amount")).as("total_sales_amount").show()
//    salesgrp.agg( max("sales_amount")).as("average_sales_amount").show()
//    salesgrp.agg( min("sales_amount")).as("maximum_sales_amount").show()
//    salesgrp.agg( avg("sales_amount")).as("minimum_sales_amount").show()

// 17th question
//    val budgets = List((1,"New Website",50000,55000),(2,"Old Software",30000,25000),(3,"New App",40000,40000),(4,"New Marketing",15000,10000),(5,"Old Campaign",20000,18000),(6,"New Research",60000,70000))
//      .toDF("project_id","project_name","budget","spent_amount")
//       val budgetdf = budgets.withColumn("budget_status",when(col("spent_amount")>"budget","Over Budget").when(col("spent_amount")==="budget","On Budget").otherwise("Under Budget"))
//    budgetdf.show()
//    budgets.filter(col("project_name").startsWith("New"))
//    val budgetgrp = budgetdf.groupBy("budget_status")
//    budgetgrp.agg( avg("spent_amount")).as("average_spent_amount").show()
//    budgetgrp.agg( max("spent_amount")).as("maximun_spent_amount").show()
//    budgetgrp.agg( min("spent_amount")).as("minimum_spent_amount").show()
 //   budgetgrp.agg( sum("spent_amount")).as("Total_spent_amount").show()

//    //18th question
//      val employee_bonus = List((1,"Sales Department",2500,"2024-01-10"),(2,"Marketing Department",1500,"2024-01-15"),(3,"IT Department",800,"2024-01-20"),(4,"HR Department",1200,"2024-02-01"),(5,"Sales Department",1800,"2024-02-10"),(6,"IT Department",950,"2024-03-01"))
//      .toDF("employee_id","department","bonus","bonus_date")
//     val bonusdf = employee_bonus.withColumn("bonus_category",when(col("bonus")>2000,"High").when(col("bonus")<=1000 && col("bonus")<=2000,"medium").otherwise("Low"))
//     bonusdf.show()
//    employee_bonus.filter(col("department").endsWith("Department"))
//    val bonusgrp = bonusdf.groupBy("bonus_category")
//    bonusgrp.agg ( sum("bonus")).as("Total_bonus").show()
//    bonusgrp.agg ( avg("bonus")).as("Average_bonus").show()
//    bonusgrp.agg ( max("bonus")).as("maximum_bonus").show()
//    bonusgrp.agg ( min("bonus")).as("minimum_bonus").show()

    //19 th question
//    val tickets = List((1,"Bug",1.5,"High"),(2,"Feature",3.0,"Medium"),(3,"Bug",4.5,"Low"),(4,"Bug",2.0,"High"),(5,"Enhancement",1.0,"Medium"))
//      .toDF("ticket_id","issue_type","resolution_time","priority")
//    val ticketdf = tickets.withColumn("resolution_status",when(col("resolution_time")<="2hours","Quick").when(col("resolution_time")<2 && col("resolution_time")<=4,"Moderate").otherwise("Slow"))
//    ticketdf.show()
//    tickets.filter(col("issue_type").contains("Bug"))
//    val ticketgrp = ticketdf.groupBy("resolution_status")
//     ticketgrp.agg (sum("resolution_time")).as("total_resolution_time")
//     ticketgrp.agg (max("resolution_time")).as("maximum_resolution_time")
//     ticketgrp.agg (min("resolution_time")).as("minimum_resolution_time")
//    ticketgrp.agg (avg("resolution_time")).as("average_resolution_time")


    //20th question
//    val event = List((1,"Tech Conference",600,"2024-01-10"),(2,"Sports Event",250,"2024-01-15"),(3,"Tech Expo",700,"2024-01-20"),(4,"Music Festival",150,"2024-02-01"))
//      .toDF("event_id","event_name","attendees","event_date")
//    val eventdf = event.withColumn("attendance_status",when(col("attendees")>500,"Full").when(col("attendees")<=200  && col("attendees")<=500,"Moderate").otherwise("Low"))
//    eventdf.show()
//    event.filter(col("event_name").startsWith("Tech"))
//    val eventgrp = eventdf.groupBy("attendance_status")
//    eventgrp.agg ( sum("attendees")).as("Total_attendees").show()
//    eventgrp.agg ( min("attendees")).as("minimum_attendees").show()
//    eventgrp.agg ( max("attendees")).as("maximum_attendees").show()
//    eventgrp.agg ( avg("attendees")).as("average_attendees").show()

   // 21th question
//    val bills = List((1,1,250,"2024-02-05"),(2,2,80,"2024-02-10"),(3,3,150,"2024-02-15"),(4,4,220,"2024-02-20"),(5,5,90,"2024-02-25"),(6,6,300,"2024-02-28"))
//      .toDF("bill_id","customer_id","bill_amount","billing_date")
//    val billsdf = bills.withColumn("bill_status",when(col("bill_amount")>200,"high").when(col("bill_amount")<=100 && col("bill_amount")<=200,"Medium").otherwise("Low"))
//    billsdf.show()
//    ////bills.filter(col("billing_date")>"February 2024")
//    val  billsgrp = billsdf.groupBy("bill_status")
//    billsgrp.agg (sum("bill_amount")).as("total_bill_amount").show()
//    billsgrp.agg (avg("bill_amount")).as("average_bill_amount").show()
//    billsgrp.agg (min("bill_amount")).as("minimum_bill_amount").show()
//    billsgrp.agg (max("bill_amount")).as("maximum_bill_amount").show()

//  22 th question
//     val store = List((1,"Widget Lite",15,"2024-01-10"),(2,"Gadget",60,"2024-01-15"),(3,"Light Lite",25,"2024-02-20"),(4,"Appliance",5,"2024-02-25"),(5,"Widget Pro",70,"2024-03-05"),(6,"Light Pro",45,"2024-03-12"))
//       .toDF("product_id","product_name", "quantity_in_stock", "last_restocked")
//    val storedf = store.withColumn("stock_status",when(col("quantity_in_stock")<20,"Critical").when(col("quantity_in_stock")<=20 && col("quantity_in_stock")<50,"Low").otherwise("sufficient"))
//    storedf.show()
//    store.filter(col("product_name").endsWith("Lite"))
//    val storegrp = storedf.groupBy("stock_status")
//    storegrp.agg( sum("quantity_in_stock")).as("tatal_quantity_in_stock").show()
//    storegrp.agg( max("quantity_in_stock")).as("maximum_quantity_in_stock").show()
//    storegrp.agg( min("quantity_in_stock")).as("minimum_quantity_in_stock").show()
//    storegrp.agg( avg("quantity_in_stock")).as("average_quantity_in_stock").show()

    // 23th question
//    val traning = List((1,1,50,"Tech"),(2,2,25,"Tech"),(3,3,15,"Management"),(4,4,35,"Tech"),(5,5,45,"Tech"),(6,6,30,"HR"))
//      .toDF("record_id", "employee_id", "training_hours", "training_type")
//    val traningdf = traning.withColumn("training_status",when(col("training_hours")>40,"Extensive").when(col("training_hours")<=20 && col("training_hours")<=40,"Moderate").otherwise("Minimal"))
//    traningdf.show()
//    traning.filter(col("training_type").startsWith("Tech"))
//    val traninggrp = traningdf.groupBy("training_status")
//    traninggrp.agg( sum("training_hours")).as("total_training_hours").show()
//    traninggrp.agg( avg("training_hours")).as("average_training_hours").show()
//    traninggrp.agg( min("training_hours")).as("minimum_training_hours").show()
//    traninggrp.agg( max("training_hours")).as("maximum_training_hours").show()

   // 24 th question
//    val register= List((1, "Alice Smith",600, "2024-03-01"),(2, "Bob Johnson" ,350, "2024-03-05"),(3,"Charlie Brown",150,"2024-03-10"),(4, "Dave Clark", 450, "2024-03-15"),(5, "Emma Wilson", 300, "2024-03-20"),(6, "Frank Miller", 700, "2024-03-25"))
//      .toDF("registration_id", "attendee_name", "registration_fee", "registration_date")
//    val registerdf = register.withColumn("fee_category",when(col("registration_fee")<=200 && col("registration_fee")<=500,"Standard").otherwise("Basic"))
//    registerdf.show()
//    ////register.filter(col("registration_date")>"March 2024")
//    val registergrp = registerdf.groupBy("fee_category")
//    registergrp.agg( sum("registration_fee")).as("total_registration_fee").show()
//    registergrp.agg( avg("registration_fee")).as("average_registration_fee").show()
//    registergrp.agg( max("registration_fee")).as("maximum_registration_fee").show()
//    registergrp.agg( min("registration_fee")).as("minimum_registration_fee").show()

   // 25 th question
//    val book_loans = List((1,"History of Rome" ,40, "2024-01-10"),(2, "Modern History" ,20, "2024-01-15"),(3, "Ancient History", 10, "2024-02-20"),(4, "European History", 15, "2024-02-25"),(5, "World History", 5,"2024-03-05"),(6, "History of Art" ,35, "2024-03-12"))
//      .toDF("loan_id", "book_title", "loan_duration_days", "loan_date")
//    val bookdf = book_loans.withColumn("loan_category",when(col("loan_duration_days")>30,"Long-term").when(col("loan_duration_days")<=15 && col("loan_duration_days")<=30,"Medium-Term").otherwise("short_term"))
//    bookdf.show()
//    book_loans.filter(col("book_title").contains("History"))
//    val bookgrp = bookdf.groupBy("loan_category")
//    bookgrp.agg( sum("loan_duration_days")).as("total_loan_duration_days").show()
//    bookgrp.agg( avg("loan_duration_days")).as("average_loan_duration_days").show()
//    bookgrp.agg( max("loan_duration_days")).as("maximum_loan_duration_days").show()
//    bookgrp.agg( min("loan_duration_days")).as("minimum_loan_duration_days").show()


}
}
