object seventh {
def main(args: Array[String]):Unit ={

  val age =63
  val isSenior = age > 60
  val isStudent = age < 25

  if (isSenior){
    println("you are eligible for a senior citizen discount")
  }else if (isStudent) {
    println("you are eligible for a student discount")
  }else{
    println("At age $age, you are not eligible for any any discount")
  }
}
}
