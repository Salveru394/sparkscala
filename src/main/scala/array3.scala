import array3.reverseArray

object array3 {
    def reverseArray(arr: Array[String]): Array[String] = {
      var n = arr.length
      var reversed = new Array[String](n)
      for (i <- 0 until n) {
        reversed(i) = arr(n - 1 - i)
      }
      reversed
    }

  def main(args: Array[String]): Unit = {
  var originalArray = Array("Mohan", "Shaker", "Ravi")
  var reversedArray = reverseArray(originalArray)

  println("original Array: " + originalArray.mkString(","))
  println("Reversed Array: " + reversedArray.mkString(", "))
}
}
