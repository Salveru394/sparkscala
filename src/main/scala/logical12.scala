object logical12 {
  def main(args: Array[String]): Unit = {

    var count = 0
    println("Enter the start of the range: ")
    var start = scala.io.StdIn.readInt()
    println("Enter the end of the range: ")
    var end = scala.io.StdIn.readInt()

    for (num <- start to end) {
      if (num % 2 == 0) {
        count = count + 1
        println(count)
      }
    }
  }
}

