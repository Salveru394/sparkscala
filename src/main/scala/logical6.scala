import scala.io.StdIn

object logical6 {
  def main(args: Array[String]): Unit = {

    println("Enter the first number")
    var num1 = StdIn.readInt()
    println("Enter the character(+,-,*,/)")
    var operator = StdIn.readChar()
    println("Enter the second number")
    var num2 = StdIn.readInt()

    var result = operator match {
      case '+' => num1 + num2
      case '-' => num1 - num2
      case '*' => num1 * num2
      case '/' => num1 / num2
      case _ => "Invalid Operator"
    }
    println(s"Result $result")
  }
}