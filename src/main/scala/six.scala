object six {
def main(args: Array[String]): Unit = {

  var num = 25

  if (isInRange(num)){
    println("The number is in range [1, 10] or [20, 30]")
  }else {
    println("The number is not in range [1, 10] or [20, 30]")
  }
def isInRange(num: Int): Boolean = {
  (1 to 10).contains(num) || (20 to 30).contains(num)
}
}
}
