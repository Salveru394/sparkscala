
object practice {
  def main(args: Array[String]): Unit = {

    println("Enter the size of array")
    val size = scala.io.StdIn.readInt()

    val arr = new Array[Int](size)

    for (i <- 0 until arr.length)
      arr(i) = scala.io.StdIn.readInt()
  }
  println("printing the array element")


  var sum = 0
  var start = 10
  var end = 25

  for (num <- start to end) {
    if (num % 15 == 0) {
      sum = sum + num
    }
    println(sum)
  }
}
