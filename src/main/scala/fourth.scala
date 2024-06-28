object fourth {
def main(args:Array[String]):Unit ={

  var age =20

  if (isEligibleToVote(age)) {
    println("At age 18, you are eligible to vote")
  }
 if (isEligibleToDrive(age)) {
   println(s"At age 16, you are eligible to drive.")
 }
}
def isEligibleToVote(age: Int): Boolean = {
  age >= 18
}
def isEligibleToDrive(age: Int): Boolean = {
  age >=16
}
}
