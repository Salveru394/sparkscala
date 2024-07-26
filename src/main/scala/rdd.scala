object rdd {
  def main(args: Array[String]): Unit = {
  }}
// normal problem                                                pyspark
//var ss = new SparkContext("local[*]", "Rahul")          ss = new SparkContext("local[*]", "Rahul")
//var rdd1 = ss.textFile("path.txt")                      rdd1 = ss.textFile("path.txt")
//var rdd2 = rdd1.flatMap(x => x.split(" "))              rdd2 = rdd1.flatMap(lambda x: x.split(" "))
//var rdd3 = rdd2.map(x =>(x,1))                          rdd3 = rdd2.map(lambda x: (x,1))
//var rdd4 = rdd3.reduceByKey((x,y)=> x+y)                rdd4 = rdd3.reduceByKey(lambda(x,y): x+y)
//rdd4.collect.foreach(println)                           rdd5 = rdd4.collect();
                                                          //for i in rdd5:
                                                          // print(i)
//var ss = new SparkContext("local[*]", "Rahul")          sort by (in place of rdd5)
//var rdd1 = ss.textFile("path.txt")                      rdd5 = rdd4.sortby(lambda x : x[1],false)
//var rdd2 = rdd1.flatMap(x => x.split(" "))              for i in rdd5.take(5):
//var rdd3 = rdd2.map(x =>(x,1))                          print(i)
//var rdd4 = rdd3.reduceByKey((x,y)=> x+y)               { in pyspark the count will start with {0,1,2,3,4,5}
//var rdd5 = rdd4.sortBy(x => x._2,false)                { if you want desc then mention after (x[1]) false}
//  rdd5.collect.foreach(println)
// tupleformat (x =>x._2,false)
// by default it will be in ascending if you want desc then mention after (x._2) false
//in scala the count will start with {1,2,3,4,5}

// parallize method {it converts array to rdd}
//var arr = Array(1,2,3,4,5)
//var data = sc.parallize(arr)
//data.collect.foreach(println)

//usind saveastextfile {used in production}
//In Drive machine we doesn't have inhalf space in that case we don't use collect we use saveastextfile
//var rdd = ss.parallelize(Array(1,2,3,4,5,6))
//  rdd.saveAsTextFile("path") {give the inpath where you want to store the data}

// using mean() to find average
//var rdd = ss.parallelize(Array(1,2,3,4,5,6))
//  var average = rdd.mean()
//println(average)

// using Reduce() to find average
//var sc = new SparkContext("local[*]","Rahul")
//var rdd = sc.parallelize(Array(1,2,3,4,5,6))
//var sum = rdd.reduce((x,y)=> x+y)
//var ct = rdd.count()
//var average = sum /ct.toDouble
//println(average)

// using filter() {we should use filter when you want even (or) count}
//var rdd = ss.parallelize(Array(1,2,3,4,5))
//  var filterrdd = rdd.filter(x =>x % 2!=0)
//var count = filterrdd.count()
//println("count of odd numbers: " +count)
//filterrdd.collect().foreach(println)
//{when filteredrdd is used, you should use count to count the numbers}

// joining two rdd's {joining the combine rows}
//var rdd1 = ss.parallelize(Array((1,"apple"),(2, "banana"),(3, "orange")))
//  var rdd2 = ss.parallelize(Array((1,"red"),(2,"yellow"),(4,"green")))
// var rdd3 = rdd1.join(rdd2)
//rdd3.collect.foreach(println)
// Out put (1,(apple,red)) (2,(banana,yellow))

// in the rdd numbers remove duplicates
//var rdd = ss.parallelize(Array(1,2,3,4,5,5,5,6,6,7,7,8,8))
// var rdd1 = rdd.distinct()
//rdd1.foreach(println) {if you want to remove duplicates use distinct()}

// set operators
// removing the elements in 1st rdd with also appear in 2nd rdd use {subtract}
//var rdd = ss.parallelize(Array(1,2,3,4,5))
//var rdd1 = ss.parallelize(Array(3,4,5,6,7))
//var rdd3 = rdd.subtract(rdd1) (or) var rdd3 = rdd1.subtract(rdd) {removing 2nd to 1st}
//rdd3.foreach(println)
// Output = (1,2) (or) (6,7)

//rdd's contains same type of elements & create an rdd contains all elements from both rdd's
//var rdd = ss.parallelize(Array(1,2,3))
//var rdd1 = ss.parallelize(Array(3,4,5))
//var rdd3 = rdd.union(rdd1)
//rdd3.foreach(println)
// Output = (1,2,3) (3,4,5) {union operator with accept's duplicates}

// output = (1 A) (1 B), (2 A) (2 B) (3 A) (3 B)
// if you find the output like this then use cartesian()
//var rdd = ss.parallelize(Array(1,2,3))
//var rdd1 = ss.parallelize(Array("A","B"))
//  var rdd3 = rdd.cartesian(rdd1)
// rdd3.foreach(println)

//given two rdd's print common element's b/w them {intersection}
//var rdd = ss.parallelize(Array(1,2,3,4,5))
//var rdd1 = ss.parallelize(Array(4,5,6,7,8))
//var rdd3 = rdd.intersection(rdd1)
//rdd3.foreach(println) { Output = 4,5 }

// searching related problems on rdd
//search for a specified value in rdd, you can use filter()
//var rdd = ss.parallelize(Array("apple","banana","orange","grep"))
//var searchterm = "orange"
//var filteredrdd = rdd.filter(x => x ==searchterm)
//filteredrdd.foreach(println)
// output = "orange" {string}

//substring (contains)
//search for a specified value in rdd, you can use filter()
//var rdd = ss.parallelize(Array("apple","banana","orange","grep"))
//var searchterm = "an"
//var filteredrdd = rdd.filter(x => x.contains(searchterm))
//filteredrdd.foreach(println)