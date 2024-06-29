object twentythree {
def main(args:Array[String]):Unit = {

   var purchaseamount = 180
  var LoyaltyCard =true

 if (purchaseamount > 200){
   print("customer is eligible for discount")
 }else if (LoyaltyCard){
   print("customer qualifies for membership benefits")
 }else {
   print("customer does not qualify for any benefits")
 }

  }

}

