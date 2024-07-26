import array9.SecondSmallest
 object array9 {
   def SecondSmallest(arr: Array[Int]): Option[Int] = {
     if (arr.length < 2) {
       None
     }
     var smallest = arr(0)
     var secondsmallest = arr(1)
     if (smallest > secondsmallest) {
       smallest = arr(1)
       secondsmallest = arr(0)
     }
     for (i <- 2 until arr.length) {
       if (arr(i) < smallest) {
         secondsmallest = smallest
         smallest = arr(i)
       } else if (arr(i) < secondsmallest && arr(i) != smallest) {
         secondsmallest = arr(i)
       }
     }
     Some(secondsmallest)
   }

   def main(args: Array[String]): Unit = {
     var arr = Array(5, 7, 8, 3, 1, 6, 4)
     var smallarr = SecondSmallest(arr)
     println(smallarr)

   }
 }