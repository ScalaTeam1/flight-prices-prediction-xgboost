package com.neu.edu.FlightPricePrediction.pojo

import com.phasmidsoftware.parse._
import com.phasmidsoftware.table.{HeadedTable, Header, Table}

case class Flight(id: Int, airline: String, flight: String, sourceCity: String, departureTime: String, stops: String, arrivalTime: String, destinationCity: String, classType: String, duration: Double, daysLeft: Int, price: Int)

object Flight extends TableParserHelper[Flight]() {

  def apply(id: Any, airline: String, flight: String, sourceCity: String, departureTime: String, stops: String, arrivalTime: String, destinationCity: String, classType: String, duration: Double, daysLeft: Int, price: Int): Flight = id match {
    case id: String =>  Flight(id, airline, flight, sourceCity, departureTime, stops, arrivalTime, destinationCity, classType, duration, daysLeft, price)
    case id: Int => Flight(id, airline, flight, sourceCity, departureTime, stops, arrivalTime, destinationCity, classType, duration, daysLeft, price)
    case _ =>  Flight(0, airline, flight, sourceCity, departureTime, stops, arrivalTime, destinationCity, classType, duration, daysLeft, price)
  }

  def camelCaseColumnNameMapper(w: String): String = w.replaceAll("([A-Z0-9])", "_$1")

  implicit val FlightsColumnHelper: ColumnHelper[Flight] = columnHelper(camelCaseColumnNameMapper _,
    "classType" -> "class"
  )

  implicit val cellParser: CellParser[Flight] = cellParser12(apply)

  implicit val parser: StandardRowParser[Flight] = StandardRowParser[Flight]

  implicit object FlightTableParser extends StringTableParser[Table[Flight]] {
    protected def builder(rows: Iterable[Flight], header: Header): Table[Flight] = HeadedTable(rows, header)

    type Row = Flight

    val maybeFixedHeader: Option[Header] = None

    override val forgiving: Boolean = true

    val rowParser: RowParser[Row, String] = implicitly[RowParser[Row, String]]

  }
}

