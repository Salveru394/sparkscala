object logical23 {
  def main(args: Array[String]): Unit = {

    var start = 5
    var end = 25
    var step = 2

    for (i <- start to end by step) {
      var square = i * i
      println(square)
    }
  }
}
// for (i <- start to end by 2)
//      {
//        square = i * i
//        println(square)

//var start = 5
  //  var end = 25
    //var step = 2

    //var current = start
    //while (current <= end){
      //var square = current * current
      //println(square)
      //current = current+step
    //}
 // }
//}