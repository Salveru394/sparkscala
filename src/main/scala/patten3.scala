object patten3 {
def main(args:Array[String]):Unit = {

var rows = 5

  for (i<- 0 until 5){
    for (j<-(rows - i) to 1 by -1){
      print("*"+" ")
    }
    print()
  }


}
}
