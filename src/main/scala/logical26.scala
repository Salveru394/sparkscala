object logical26 {
  def main(args: Array[String]): Unit = {

    var start = 1
    var end = 35
    for (i <- 1 to 35) {
      if (i % 2 == 1) {
        println(i)
      } else {
        println("even")
      }
      if (i < 35) {
        println(",")
      }
    }
    println()
  }
}