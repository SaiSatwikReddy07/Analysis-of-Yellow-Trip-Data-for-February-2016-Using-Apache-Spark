
import org.apache.spark.SparkContext
import java.time._
import java.time.format.DateTimeFormatter
import scala.math._
import breeze.plot._
import breeze.linalg._

object Testing {
  val sc =new SparkContext("local","Test")
  def main(args:Array[String]):Unit={
    val file_path="the directory where the csv is stored"
    val rdd=sc.textFile(file_path).map(x => x.split(","))
    val labels=rdd.collect()(0)
    val data=rdd.collect().drop(1)
//    vendor_id_count(data)
//    trip_duration(data)
//    payment_type(data)
//    fare_amount(data)
//    trip_distance(data)
//    passenger_count(data)
//    ratecode_id(data)
//    tip_amount(data)
//    trip_type(data)
//    revenue_calc(data)
//    calculate_peak_booking_hour(data)
  }

  def vendor_id_count(data:Array[Array[String]]):List[String]={
    var out:List[String]=List.empty[String]
    var d: Array[(String, Int)] = data.flatMap(x => {
      x.zipWithIndex.filter { case (y, index) => index == 0 }.map(x => (x._1, 1))
    })
    var count = sc.parallelize(d).reduceByKey(_ + _).collect()
   // println(s"Total trips = ${d.length}")
    out=out:+s"Total trips = ${d.length}"
    count.foreach(x => out=out:+s"Vendor id - ${x._1} has total ${x._2} trips" )

    return out
  }

  def trip_duration(data:Array[Array[String]]):List[String]={
      var out:List[String]=List.empty[String]
       var d: Array[Array[String]] = data.map(x => {
         x.zipWithIndex.filter { case (y, index) => index == 1 || index == 2 }.map(x => (x._1)) })
       var duration:List[Float]=List.empty[Float]
      d.foreach(x =>{
        val pickup_time=LocalDateTime.parse(x(0),DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val dropoff_time=LocalDateTime.parse(x(1),DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        duration=duration:+ Duration.between(pickup_time,dropoff_time).toSeconds.toFloat/60 })
      val average_duration=(duration.sum)/duration.length
      val longest=duration.max
      val shortest=duration.min


    out=out:+s"Average Trip Duration : ${average_duration} min"
    out=out:+s"Longest Trip Duration : ${longest} min Between ${d(duration.indexOf(longest))(0)} " + s"and ${d(duration.indexOf(longest))(1)}"
    out=out:+s"Shortest Trip Duration : ${shortest} min Between ${d(duration.indexOf(shortest))(0)} " + s"and ${d(duration.indexOf(shortest))(1)}"
      //println(s"Average Trip Duration : ${average_duration} min")
      //println(s"Longest Trip Duration : ${longest} min Between ${d(duration.indexOf(longest))(0)} " + s"and ${d(duration.indexOf(longest))(1)}")
      //println(s"Shortest Trip Duration : ${shortest} min Between ${d(duration.indexOf(shortest))(0)} " + s"and ${d(duration.indexOf(shortest))(1)}")
return out
      }

  def payment_type(data:Array[Array[String]]):List[String]={
    var out:List[String]=List.empty[String]
    var d:Array[Int]= data.flatMap(x => {
      x.zipWithIndex.filter { case (y, index) => index == 20 }.map(x => (x._1.toInt)) })

    var d1=d.map(x => (x,1))
    var types=sc.parallelize(d1).reduceByKey(_+_).collect()
    types.foreach(x=>{
      val p:Float=(x._2.toFloat/d.length.toFloat)*100
      //println(s"${p}% passengers used type - ${x._1} payment system")
      out=out:+s"${p}% passengers used type - ${x._1} payment system"
    })

    val hist = breeze.plot.hist(DenseVector(d))
    val f = Figure()
    val p = f.subplot(0)
    p += hist
    p.xlabel = "Payment Type"
    p.ylabel = "Frequency"
    p.title = "Payment Type Distribution"
    f.refresh()

    return out
  }


  def fare_amount(data: Array[Array[String]]): List[String] = {
    var out:List[String]=List.empty[String]
    var e: Array[Float] = data.flatMap(x => {
      x.zipWithIndex.filter { case (y, index) => index == 13 }.map(x => (x._1.toFloat)) })
    val average=e.sum/e.length
    val max=e.max
    val min=e.min
//    println(s"Average Fare Amount : ${average}")
//    println(s"Maximum Fare Amount : ${max}")
//    println(s"Minimum Fare Amount : ${min}")
    out=out:+s"Average Fare Amount : ${average}"
    out=out:+s"Maximum Fare Amount : ${max}"
    out=out:+s"Minimum Fare Amount : ${min}"
    return out
  }

  def trip_distance(data: Array[Array[String]]): List[String] = {
    var out:List[String]=List.empty[String]
    var location: Array[Array[Double]] = data.map(x => {
      x.zipWithIndex.filter { case (y, index) => index == 5 || index ==6 || index==8 || index==9 }
        .map(x => (x._1.toDouble)) })
    var upadated=location.filter(x => !x.contains(0.0))
    var distance:List[Double]=List.empty[Double]
    upadated.foreach(x=>{
      val pickup_longitude=x(0)
      val pickup_latitude=x(1)
      val dropoff_longitude=x(2)
      val dropoff_latitude=x(3)
      val dlon=toRadians(dropoff_longitude-pickup_longitude)
      val dlat=toRadians(dropoff_latitude-pickup_latitude)
      val a = sin(dlat / 2) * sin(dlat / 2) +
        cos(toRadians(pickup_latitude)) * cos(toRadians(dropoff_latitude)) * sin(dlon / 2) * sin(dlon / 2)
      val c = 2 * atan2(sqrt(a), sqrt(1 - a))
      distance=distance:+(6371*c)
    })
    val average_distance=distance.sum/distance.length
    val max=distance.max
    val min=distance.min
//    println(s"Average Distance : ${average_distance}km")
//    println(s"Maximum Distance : ${max}km")
//    println(s"Minimum Distance : ${min}km")

    out=out:+s"Average Distance : ${average_distance}km"
    out=out:+s"Maximum Distance : ${max}km"
    out=out:+s"Minimum Distance : ${min}km"
    return out
  }

  def passenger_count(data:Array[Array[String]]):List[String]={
    var out:List[String]=List.empty[String]
    var d:Array[Int]= data.flatMap(x => {
      x.zipWithIndex.filter { case (y, index) => index == 11}
        .map(x => (x._1.toInt))
    })
    var d1=d.map(x => (x,1))
    var count=sc.parallelize(d1).reduceByKey(_+_).collect()
    count.foreach(x=>{
      val p: Float = (x._2.toFloat / d.length.toFloat) * 100
      //println(s"${p}% trips are travelled with ${x._1} passengers")
      out=out:+s"${p}% trips are travelled with ${x._1} passengers"
    })

    val hist = breeze.plot.hist(DenseVector(d))
    val f = Figure()
    val p = f.subplot(0)
    p += hist
    p.xlabel = "Passengers"
    p.ylabel = "Frequency"
    p.title = "Passenger count"
    f.refresh()

    return out
  }

  def ratecode_id(data:Array[Array[String]]):List[String]= {
    var out:List[String]=List.empty[String]
    var ratecode_id: Array[Array[String]] = data.map(x => {
      x.zipWithIndex.filter { case (y, index) => index == 0 || index == 4 }.map(x => ((x._1)))
    })
    var count =ratecode_id.map(x =>(x.mkString(","),1))
    count=sc.parallelize(count).reduceByKey(_+_).collect()
    count.sortBy(_._1).foreach(k =>{
      val key=k._1.split(",")
      val v_id = key(0)
      val r_id=key(1)
      val v=k._2
//      println(s"Vendor id - ${v_id}")
//      println(s"${v} trips used rate code id - ${r_id} for fare calculation")
      out=out:+s"Vendor id - ${v_id}"
      out=out:+s"${v} trips used rate code id - ${r_id} for fare calculation"
   })
    return out
  }

  def tip_amount(data: Array[Array[String]]): List[String] = {
    var out:List[String]=List.empty[String]
    var amount: Array[Float] = data.flatMap(x => {
      x.zipWithIndex.filter { case (y, index) => index == 16}.map(x => (x._1.toFloat))
    })
    val max_amount=amount.max
    val min_amount=amount.min
    val avg=amount.sum/amount.length
//    println(s"Average Tip Amount : ${avg}")
//    println(s"Maximum Tip Amount : ${max_amount}")
//    println(s"Minimum Tip Amount : ${min_amount}")

    out=out:+s"Average Tip Amount : ${avg}"
    out=out:+s"Maximum Tip Amount : ${max_amount}"
    out=out:+s"Minimum Tip Amount : ${min_amount}"

    val hist = breeze.plot.hist(DenseVector(amount))
    val f = Figure()
    val p = f.subplot(0)
    p += hist
    p.xlim(0.0,max_amount)
    p.xlabel = "Tip Amount"
    p.ylabel = "Frequency"
    p.title = "Tip Amount Distribution"
    f.refresh()
return out
  }

  def trip_type(data: Array[Array[String]]): List[String] = {
    var out:List[String]=List.empty[String]
    var trip: Array[Int] = data.flatMap(x => {
      x.zipWithIndex.filter { case (y, index) => index == 21 }.map(x => (x._1.toInt))
    })
    var triptype=sc.parallelize(trip.map(x => (x,1))).reduceByKey(_+_).sortBy(_._1).collect()
    triptype.foreach(x => {
//        println(s"Trip Type ${x._1} ")
//        println(s"Total Trips - ${x._2}")
      out=out:+s"Trip Type ${x._1} "
      out=out:+s"Total Trips - ${x._2}"
    })
    val hist = breeze.plot.hist(DenseVector(trip))
    val f = Figure()
    val p = f.subplot(0)
    p += hist
    p.xlabel = "Trip Type"
    p.ylabel = "Frequency"
    p.title = "Trip Type Distribution"
    f.refresh()
return out
  }

  def revenue_calc(data: Array[Array[String]]):List[String]={
    var out:List[String]=List.empty[String]
    var d: Array[Array[Float]] = data.map(x => {
      x.zipWithIndex.filter { case (y, index) => index==0||index==13||index==14||index==15 }
        .map(x => (x._1.toFloat)) })

    var r=d.map(x=> (x(0),x.slice(1,3).sum-x(3)))
    val r1=sc.parallelize(r).reduceByKey(_+_).collect()
    r1.foreach(x => {
//      println(s"Vendor id - ${x._1}")
//      println(s"Total revenue generated - ${x._2}USD")
      out=out:+s"Vendor id - ${x._1}"
      out=out:+s"Total revenue generated - ${x._2}USD"
    })
return out
  }

  def calculate_peak_booking_hour(data:Array[Array[String]]):List[String]={
    var out:List[String]=List.empty[String]
    var d: Array[String] = data.flatMap(x => {
      x.zipWithIndex.filter { case (y, index) =>index == 1}.map(x => (x._1)) })
    var h:List[String]=List.empty[String]
    d.foreach(x =>{
      val time=LocalDateTime.parse(x,DateTimeFormatter.ISO_LOCAL_DATE_TIME).getHour
      h=h:+time.toString
    })
    val c1=sc.parallelize(h.map(x => (x,1))).reduceByKey(_+_).sortBy(_._2).collect()
    val c=c1(c1.length-1)._1.toInt
    if (c <12) {
//      println(s"Taxis are busiest during the time interval between ${c} AM and ${c+1} AM")
      out=out:+s"Taxis are busiest during the time interval between ${c} AM and ${c+1} AM"
    }
     else if(c>12) {
     // println(s"Taxis are busiest during the time interval between ${c-12} PM and ${(c+1)-12} PM")
    out=out:+s"Taxis are busiest during the time interval between ${c-12} PM and ${(c+1)-12} PM"
  }
    return out
  }
}