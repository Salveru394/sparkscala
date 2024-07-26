object array7 {
def main(args: Array[String]): Unit = {

  def containsString(arr: Array[String], target: String): Boolean = {
      arr.exists(_==target)
    }
    var array1 = Array("Mohan", "hari", "mahi")
    var target1 = "hari"
    println(s"Array contains '$target1': ${containsString(array1, target1)}")
}
}
