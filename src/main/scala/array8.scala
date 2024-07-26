object array8 {
def main(args: Array[String]): Unit = {

  var ele = Array(1,2,3,4,5)

  for (ele <- 1 to 5) {
    if (ele % 2 == 1){
      println(ele)
    }
  }
}
}
// var arr = Array(1,2,3,4,5)
// for (arr <- 1 to 5){
// if (arr % 2 !=0){
//println(arr)