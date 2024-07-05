object logical3 {
def main(args:Array[String]):Unit = {

  var a = 10
  var b= 30
  var c = 20

  if (a >=b && a >=c){
    print("A is greater")
  }else if (b >=a && b >=c){
    print("B is greater")
  }else {
    print("C is greater")
  }
}
}
