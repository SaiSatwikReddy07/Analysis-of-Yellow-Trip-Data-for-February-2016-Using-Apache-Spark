import scala.swing._
import scala.swing.event._
import Analysis._
import org.apache.spark.SparkContext
import scala.math._
import breeze.plot._
import breeze.linalg._
object b  {
  def main(args: Array[String]): Unit = {
    val file_path = "the directory where the csv is stored"
    val rdd = sc.textFile(file_path).map(x => x.split(","))
    val labels = rdd.collect()(0)
    val data = rdd.collect().drop(1)
          val frame = new MainFrame {
          title = "Taxi Data Analytics"

          size = new Dimension(300, 200)
          val tripduration = new Button{text = "trip_duration"}
          tripduration.maximumSize = new Dimension(150, 30)
          val paymenttype = new Button{text = "payment_type"}
          paymenttype.maximumSize = new Dimension(150, 30)
          val vendorid_count = new Button{text = "vendor_id_count"}
          vendorid_count.maximumSize = new Dimension(150, 30)
          val fareamount = new Button{text = "fare_amount"}
          fareamount.maximumSize = new Dimension(150, 30)
          val tripdistance = new Button{text = "trip_distance"}
          tripdistance.maximumSize = new Dimension(150, 30)
          val passengercount = new Button{text = "passenger_count"}
          passengercount.maximumSize = new Dimension(150, 30)
          val ratecodeid = new Button{text = "ratecode_id"}
          ratecodeid.maximumSize = new Dimension(150, 30)
          val tipamount = new Button{text = "tip_amount"}
          tipamount.maximumSize = new Dimension(150, 30)
          val triptype = new Button{text = "trip_type"}
          triptype.maximumSize = new Dimension(150, 30)
          val revenuecalc = new Button{text = "revenue_calc"}
          revenuecalc.maximumSize = new Dimension(150, 30)
          val calculatepeak_booking_hour = new Button{text = "calculate_peak_booking_hour"}
          calculatepeak_booking_hour.maximumSize = new Dimension(150, 30)
          val Output = new TextArea{
            rows = 25
            columns = 40
          }
          val t_d = new BoxPanel(Orientation.Horizontal){
            contents += tripduration
            contents += new Label{text = "   Gives Longest,Shortest,Average Trip Duration "}
          }
          val p_t = new BoxPanel(Orientation.Horizontal) {
            contents += paymenttype
            contents += new Label {
              text = "   Gives Percentage of payments made through cash and card "
            }
          }
          val v_i_c = new BoxPanel(Orientation.Horizontal) {
            contents += vendorid_count
            contents += new Label {
              text = "   Gives Total trips,Number of trips by vendor 1,Number of trips by vendor 2"
            }
          }
          val f_a = new BoxPanel(Orientation.Horizontal) {
            contents += fareamount
            contents += new Label {
              text = "   Gives Maximum,Minimum,Average fare amount "
            }
          }
          val t_dist = new BoxPanel(Orientation.Horizontal) {
            contents += tripdistance
            contents += new Label {
              text = "   Gives Maximum,Minimum,Average Distance "
            }
          }
          val p_c = new BoxPanel(Orientation.Horizontal) {
            contents += passengercount
            contents += new Label {
              text = "   Gives Percentage of trips with specified number of passengers   "
            }
          }
          val r_id = new BoxPanel(Orientation.Horizontal) {
            contents += ratecodeid
            contents += new Label {
              text = "   Gives Number of times each rate code id by specific vendor taxi trip is used for fare calculation."
            }
          }
          val t_a = new BoxPanel(Orientation.Horizontal) {
            contents += tipamount
            contents += new Label {
              text = "   Gives Maximum,Minimum,Average Tip amount"
            }
          }
          val t_t = new BoxPanel(Orientation.Horizontal) {
            contents += triptype
            contents += new Label {
              text = "   Gives Percentage of regular taxi or street-hails,pre-arranged taxi or non-street hails"
            }
          }
          val r_c = new BoxPanel(Orientation.Horizontal) {
            contents += revenuecalc
            contents += new Label {
              text = "   Gives The total revenue of vendor 1 and vendor 2."
            }
          }
          val c_p = new BoxPanel(Orientation.Horizontal) {
            contents += calculatepeak_booking_hour
            contents += new Label {
              text = "   Gives the time interval in which most of the taxis are hired."
            }
          }
          val Analytics = new GridPanel(12,1) {
            contents += t_d
            contents += p_t
            contents += v_i_c
            contents += f_a
            contents += t_dist
            contents += p_c
            contents += r_id
            contents += t_a
            contents += t_t
            contents += r_c
            contents += c_p
            contents += Output
          }
          listenTo(tripduration,paymenttype,vendorid_count,fareamount,tripdistance,passengercount,ratecodeid,tipamount,triptype,revenuecalc,calculatepeak_booking_hour
          )
          reactions +={
            case ButtonClicked(component) =>
              if (component == tripduration) {
                var output = trip_duration(data)
                println(output.mkString("\n"))
                Output.text = output.mkString("\n")
              }
               else if (component == paymenttype) {
                var output = payment_type(data)
                Output.text = output.mkString("\n")
                println(output.mkString("\n"))
              }
               else if (component == vendorid_count) {
                var output = vendor_id_count(data)
                Output.text = output.mkString("\n")
                println(output.mkString("\n"))
              }
               else if (component == fareamount) {
                var output = fare_amount(data)
                Output.text = output.mkString("\n")
                println(output.mkString("\n"))
              } else if (component == tripdistance) {
                var output = trip_distance(data)
                Output.text = output.mkString("\n")
                println(output.mkString("\n"))
              } else if (component == passengercount) {
                var output = passenger_count(data)
                Output.text = output.mkString("\n")
                println(output.mkString("\n"))
              }else if (component == ratecodeid) {
                var output = ratecode_id(data)
                Output.text = output.mkString("\n")
                println(output.mkString("\n"))
              } else if (component == tipamount) {
                var output = tip_amount(data)
                Output.text = output.mkString("\n")
                println(output.mkString("\n"))
              } else if (component == triptype) {
                var output = trip_type(data)
                Output.text = output.mkString("\n")
                println(output.mkString("\n"))
              } else if (component == revenuecalc) {
                var output = revenue_calc(data)
                Output.text = output.mkString("\n")
                println(output.mkString("\n"))
              } else if (component == calculatepeak_booking_hour) {
                var output = calculate_peak_booking_hour(data)
                Output.text = output.mkString("\n")
                println(output.mkString("\n"))
              }
          }
          contents = Analytics
        }
        frame.visible = true
      }
}