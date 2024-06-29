object seventeen {
def main(args:Array[String]):Unit = {

  var age = 70
  var isSenior = age > 60
  var isStudent = age < 25

  if (isSenior){
    println("you are eligible for senior citizen discount")
 }else if (isStudent) {
    println("you are eligible for student discount")
  }else {
    println("At the age $age you are not eligible for any discount")
  }
}
}
