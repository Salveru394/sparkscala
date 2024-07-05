object patten5 {
def main(args:Array[String]):Unit = {
val rows = 5

    for (i <-0 until rows){

     var rowstring = " "
     for (j <-0 to i){
       if (j==i){
         rowstring +="*"
       }else {
         rowstring +="*_"
       }
     }
      println(rowstring)
  }
}
}
