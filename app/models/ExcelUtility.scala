package models

import com.github.nscala_time.time.Imports._
import org.apache.poi.openxml4j.opc._
import org.apache.poi.ss.usermodel._
import org.apache.poi.xssf.usermodel._
import play.api.Logger

import java.io._
import java.nio.file.{Files, _}
import javax.inject._

@Singleton
class ExcelUtility @Inject()
(environment: play.api.Environment,monitorTypeOp: MonitorTypeOp){
  val docRoot = environment.rootPath + "/report_template/"

  private def prepareTemplate(templateFile: String) = {
    val templatePath = Paths.get(docRoot + templateFile)
    val reportFilePath = Files.createTempFile("temp", ".xlsx");

    Files.copy(templatePath, reportFilePath, StandardCopyOption.REPLACE_EXISTING)

    //Open Excel
    val pkg = OPCPackage.open(new FileInputStream(reportFilePath.toAbsolutePath().toString()))
    val wb = new XSSFWorkbook(pkg);

    (reportFilePath, pkg, wb)
  }

  def finishExcel(reportFilePath: Path, pkg: OPCPackage, wb: XSSFWorkbook) = {
    val out = new FileOutputStream(reportFilePath.toAbsolutePath().toString());
    wb.write(out);
    out.close();
    pkg.close();

    new File(reportFilePath.toAbsolutePath().toString())
  }
  
  import controllers.Highchart._
  def exportChartData(chart: HighchartData, monitorTypes: Array[String], showSec:Boolean): File = {
    val precArray = monitorTypes.map { mt => monitorTypeOp.map(mt).prec }
    exportChartData(chart, precArray, showSec)
  }

  def exportChartData(chart: HighchartData, precArray: Array[Int], showSec:Boolean) = {
    val (reportFilePath, pkg, wb) = prepareTemplate("chart_export.xlsx")
    val evaluator = wb.getCreationHelper().createFormulaEvaluator()
    val format = wb.createDataFormat();

    val sheet = wb.getSheetAt(0)
    val headerRow = sheet.createRow(0)
    headerRow.createCell(0).setCellValue("時間")

    var pos = 0
    for {
      col <- 1 to chart.series.length
      series = chart.series(col - 1)
    } {
      headerRow.createCell(pos+1).setCellValue(series.name)
      pos+=1
    }

    val styles = precArray.map { prec =>
      val format_str = "0." + "0" * prec
      val style = wb.createCellStyle();
      style.setDataFormat(format.getFormat(format_str))
      style
    }

    // Categories data
    if (chart.xAxis.categories.isDefined) {
      val timeList = chart.xAxis.categories.get
      for (row <- timeList.zipWithIndex) {
        val rowNo = row._2 + 1
        val thisRow = sheet.createRow(rowNo)
        thisRow.createCell(0).setCellValue(row._1)

        for {
          col <- 1 to chart.series.length
          series = chart.series(col - 1)
        } {
          val cell = thisRow.createCell(col)
          cell.setCellStyle(styles(col - 1))

          val pair = series.data(rowNo - 1)
          if (pair.length == 2 && pair(1).isDefined) {
            cell.setCellValue(pair(1).get)
          }
          //val pOpt = series.data(rowNo-1)
          //if(pOpt.isDefined){
          //  cell.setCellValue(pOpt.get)
          //}

        }
      }
    } else {
      val rowMax = chart.series.map(s => s.data.length).max
      for (row <- 1 to rowMax) {
        val thisRow = sheet.createRow(row)
        val timeCell = thisRow.createCell(0)
        pos = 0
        for {
          col <- 1 to chart.series.length
          series = chart.series(col - 1)
         } {
          val cell = thisRow.createCell(pos +1)
          pos +=1
          cell.setCellStyle(styles(col - 1))

          val pair = series.data(row - 1)
          if (col == 1) {
            val dt = new DateTime(pair(0).get.toLong)
            if(!showSec)
              timeCell.setCellValue(dt.toString("YYYY/MM/dd HH:mm"))
            else
              timeCell.setCellValue(dt.toString("YYYY/MM/dd HH:mm:ss"))
          }
          if (pair(1).isDefined) {
            cell.setCellValue(pair(1).get)
          }                    
        }
      }
    }

    finishExcel(reportFilePath, pkg, wb)
  }

  def getAuditReports(auditLog: AuditLog, terminalMap: Map[Int, String]) ={
    val (reportFilePath, pkg, wb) = prepareTemplate("auditReport.xlsx")
    val format = wb.createDataFormat()
    val year = auditLog._id.reportID.airpotInfoID.year + 1911
    val quarter = auditLog._id.reportID.airpotInfoID.quarter
    val start = new LocalDate(year, 1 + 3* quarter, 1).toLocalDateTime(LocalTime.MIDNIGHT)
    val end = start.plusMonths(3).minusSeconds(1)
    val mntNum = auditLog._id.mntNum
    def fillSecAuditReport(): Unit ={
      val sheet = wb.getSheetAt(0)
      // Just take first 500
      val logs = auditLog.logs.filter(p=>p.dataType == AuditLog.DataTypeNoiseSec).take(500)
      sheet.getRow(0).getCell(0)
        .setCellValue(s"稽核資料：${terminalMap(mntNum)}每秒噪音監測資料")

      sheet.getRow(1).getCell(0)
        .setCellValue(s"稽核時間：${start.toString("yyyy/MM/dd")}到${end.toString("yyyy/MM/dd")} 23:59:59的每秒資料")

      for((log, idx)<- logs.zipWithIndex){
        val row = sheet.createRow(4 + idx)
        var cell = row.createCell(0)
        val dt = new DateTime(log.time)
        cell.setCellValue(dt.toString("YYYY/MM/dd HH:mm"))
        cell = row.createCell(1)
        cell.setCellValue(log.msg)
      }
    }

    def fillNoiseEventReport(): Unit ={
      val sheet = wb.getSheetAt(1)
      val logs = auditLog.logs.filter(p=>p.dataType == AuditLog.DataTypeNoiseEvent).take(500)
      sheet.getRow(0).getCell(0)
        .setCellValue(s"稽核資料：${terminalMap(mntNum)}事件監測資料")

      sheet.getRow(1).getCell(0)
        .setCellValue(s"稽核時間：${start.toString("yyyy/MM/dd")}到${end.toString("yyyy/MM/dd")} 23:59:59的事件監測資料")

      for((log, idx)<- logs.zipWithIndex){
        val row = sheet.createRow(4 + idx)
        var cell = row.createCell(0)
        val dt = new DateTime(log.time)
        cell.setCellValue(dt.toString("YYYY/MM/dd HH:mm"))
        cell = row.createCell(1)
        cell.setCellValue(log.msg)
      }
    }

    def fillNoiseHourReport(): Unit ={
      val sheet = wb.getSheetAt(2)
      val logs = auditLog.logs.filter(p=>p.dataType == AuditLog.DataTypeNoiseHour)
      sheet.getRow(0).getCell(0)
        .setCellValue(s"稽核資料：${terminalMap(mntNum)}每小時資料")

      sheet.getRow(1).getCell(0)
        .setCellValue(s"稽核時間：${start.toString("yyyy/MM/dd")}到${end.toString("yyyy/MM/dd")} 23:59:59 每小時資料")

      for((log, idx)<- logs.zipWithIndex){
        val row = sheet.createRow(16 + idx)
        var cell = row.createCell(0)
        val dt = new DateTime(log.time)
        cell.setCellValue(dt.toString("YYYY/MM/dd HH:mm"))
        cell = row.createCell(1)
        cell.setCellValue(log.msg)
      }
    }

    def fillNoiseDayReport(): Unit ={
      val sheet = wb.getSheetAt(3)
      val logs = auditLog.logs.filter(p=>p.dataType == AuditLog.DataTypeNoiseDay)
      sheet.getRow(0).getCell(0)
        .setCellValue(s"稽核資料：${terminalMap(mntNum)}每日噪音監測資料")

      sheet.getRow(1).getCell(0)
        .setCellValue(s"稽核時間：${start.toString("yyyy/MM/dd")}到${end.toString("yyyy/MM/dd")} 23:59:59 每小時資料")

      for((log, idx)<- logs.zipWithIndex){
        val row = sheet.createRow(16 + idx)
        var cell = row.createCell(0)
        val dt = new DateTime(log.time)
        cell.setCellValue(dt.toString("YYYY/MM/dd"))
        cell = row.createCell(1)
        cell.setCellValue(log.msg)
      }
    }

    fillSecAuditReport()
    fillNoiseEventReport()
    fillNoiseHourReport()
    fillNoiseDayReport()

    val result =finishExcel(reportFilePath, pkg, wb)
    val targetFile = new File(result.toPath.getParent + s"/${terminalMap(mntNum)}.xlsx")

    if(!result.renameTo(targetFile)) {
      Logger.error(s"faile to rename to ${targetFile}")
      result
    }else
      targetFile
  }
}