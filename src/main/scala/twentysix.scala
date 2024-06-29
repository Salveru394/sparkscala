object twentysix {
def main(args:Array[String]):Unit = {

  var age = 70
  var isSeniorCitizen = age > 65
  var NewCustomer = false

  if (isSeniorCitizen && !NewCustomer){
   println("person is eligible for a senior citizen discount")
  }else {
    println("person is not eligible for a senior citizen discount")
  }
}
}
