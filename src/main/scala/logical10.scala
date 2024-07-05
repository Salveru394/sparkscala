object logical10 {
def main(args:Array[String]):Unit = {

  var start = 700
  var end   = 900

  for (num <- start to end){
    if (num % 2 == 0){
      println(num)
    }
  }
}
}
