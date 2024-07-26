import array6.findAverage
object array6 {
  def findAverage(arr: Array[Int]): Double = {
    if (arr.isEmpty) {
      None
    }
    var sum = arr.sum.toDouble
    var count = arr.length
    var average = sum / arr.length.toDouble
    average
  }
  def main(args: Array[String]): Unit = {
      var array1 = Array(1, 2, 3, 4, 5)
      var average1 = findAverage(array1)
      println(s"Average of array1: $average1")
}
}
//if (arr.isEmpty)
//{
//None
  //  }
 //var sum = arr.sum.toDouble
 //var count = arr.length
 //var average = sum.toDouble / count
//average
  //}
  //def main(args: Array[String]): Unit  ={
  //var arr = Array(10,20,40,80,140)
    //var averagearr = Findaverage(arr)
    //println(averagearr)

// var arr = Array(10,20,30,40,50)
// var sum = (10 to 50 by 2).sum
// var count = (10 to 50 by 2).length
// var average = sum.toDouble / count
// println(average)