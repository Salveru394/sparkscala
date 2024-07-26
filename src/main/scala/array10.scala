import array10.isSortedAscending
object array10 {
  def isSortedAscending(arr: Array[Int]): Boolean = {
    for (i <- 0 until arr.length - 1) {
      if (arr(i) < arr(i + 1)) {
        return false
      }
    }
    true
  }

  def main(args: Array[String]): Unit = {
    var arr1 = Array(5, 4, 3, 2, 1)
    var arr2 = Array(1, 3, 2, 4, 5)
    var ascendarr1 = isSortedAscending(arr1)
    var ascendarr2 = isSortedAscending(arr2)
    println(ascendarr1)
    println(ascendarr2)
  }
}

//import prac3.isSortedAscending
//object prac3 {
  //def isSortedAscending(arr: Array[Int]): Array[Int] = {
    //var n = arr.length
    //for (i <- 0 until n) {
      //var swapped = false
      //for (j <- 0 until (n - i - 1)) {
        //if (arr(j) > arr(j + 1)) {
          //var temp = arr(i)
          //arr(j) = arr(j + 1)
          //arr(j + 1) = temp
         // swapped = true
        //}
        //if (!swapped) {
          //return arr
        //}
      //}
    //}
    //arr
  //}
  //def main(args: Array[String]): Unit = {
    //var arr1 = Array(5, 4, 3, 2, 1)
    //var arr2 = Array(1, 2, 3, 4, 5)
    //var ascendarr1 = isSortedAscending(arr1)
    //var ascendarr2 = isSortedAscending(arr2)
    //println(isSortedAscending(arr1))
    //println(isSortedAscending(arr2))
  //}
//}


