object fifteen {
def main(args:Array[String]):Unit = {

  var age = 15
  if (age < 13) {
    println("person is a child")
  }else if (age >=13 && age <= 19){
    println("person is a teenager")
  }else {
    println("person is an adult")
  }
  //Using logical OR
  if  (age < 13) {
    println("person is a child")
  }else if (age >=13 || age <=19) {
    println("person is a teenager")
  }else {
    println("person is an adult")
  }
}
}
