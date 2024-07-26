object array2 {
  def main(args: Array[String]): Unit = {

    var arr = Array(10, 20, 30, 40, 50)
    var sum = 0

      for (ele <- arr) {
      sum = sum + ele
      println(sum)
    }
  }
}
