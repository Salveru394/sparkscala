object logical9 {
  def main(args: Array[String]): Unit = {

    var start = 56
    var end = 153
    var sum = 0
    for (num <- start to end) {
      sum =sum + num
    }
    println(sum)
  }
}

//   var sum = 0
//  for (num <- 56 to 153){
//    sum = num + sum
//     println(sum)
