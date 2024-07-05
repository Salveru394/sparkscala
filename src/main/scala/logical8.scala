object logical8 {
def main(args:Array[String]):Unit = {

  for (num <- 250 to 550){
    if (num % 11 == 0){
      println(num)
    }
  }
}
}
