val topic = "WECC/SAQ200/861108035980803/sensor"
val topic1 = "861108035980803/sensor"
val pattern = "WECC/SAQ200/([0-9]+)/sensor".r
val pattern1 = "WECC/SAQ200/([0-9]+)/.*".r
val pattern1(a) =  topic

val str= "\02"
val v = str.getBytes
print(v(0))