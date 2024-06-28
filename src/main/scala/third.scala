object third{
  def main(args:Array[String]):Unit ={
    val number = 18

      // Check divisibility by 4 or 6
      if (isDivisibleBy4Or6(number)) {
        println("$number is divisible by either 4 or 6")
      }else {
        println("$number is not divisible by either 4 or 6")
      
    }
     def isDivisibleBy4Or6(num: Int): Boolean = {
      (num % 4 == 0) || (num % 6 == 0)
  }
  }

}
