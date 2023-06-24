# Analysis of Yellow Trip Data for February 2016 Using Apache Spark

## Project Overview
1. This project aims to analyze the Yellow Trip Data for the month of February 2016 using Apache Spark. The Yellow Trip Data is a dataset containing information about taxi trips in New York City. Additionally, a graphical user interface (GUI) has been developed to display the analysis results and plots.
2. The analysis is carried out using Apache Spark, a powerful distributed computing framework. Spark allows for efficient processing of large-scale datasets, enabling us to extract meaningful insights and perform various calculations on the Yellow Trip data.
3. To enhance the user experience and facilitate data visualization, a GUI has been developed. The GUI provides an intuitive interface to interact with the analysis results and view plots generated from the data.

## Setup and Dependencies
1. Apache spark 3.4.0
2. Ubuntu 22.04
3. scala.swing 3.0.0
4. java.time (Java 8)
5. scala.math
6. Breeze


## Dataset Description
The dataset used in this analysis is the Yellow Trip Data for February 2016. It includes details about each taxi trip, such as pickup and drop-off locations, trip duration, passenger count, and more. The dataset is available in CSV format and will be processed using Apache Spark to gain insights and perform various analyses.
<b>Source: <a href="http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml"> Click here</a></B>

## Analysed Stats:
1. The longest trip duration, shortest trip Duration, and average trip duration.
2. The percentage of payments made through cash (represented by ‘1’ in the dataset) and percentage of payments made through card (represented by ‘2’ in the dataset).
3. The total trips, number of trips by vendor 1 and number of trips by vendor 2.
4. The maximum fare amount, minimum fare amount, and average fare amount.
5. The maximum distance, minimum distance, and average distance.
6. The percentage of trips with specified number of passengers.
7. The number of times each rate code id by specific vendor taxi trip is used for fare calculation.
8. The maximum tip amount, minimum tip amount, and average tip amount.
9. The percentage of regular taxi or street-hails and percentage of pre-arranged taxi or non-street hails.
10. The total revenue of vendor 1 and vendor 2.
11. The time interval (the peak booking hour) in which most of the taxis are hired.


## Before you run the code
Enter the directory of the dataset (the csv file) in the file_path variable before you run the code.




