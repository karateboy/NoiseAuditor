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
(environment: play.api.Environment){
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

  def getAuditReports(auditLog: AuditLog, terminalMap: Map[Int, String]) ={
    val (reportFilePath, pkg, wb) = prepareTemplate("auditReport.xlsx")
    val format = wb.createDataFormat()
    val year = auditLog._id.reportID.airpotInfoID.year + 1911
    val quarter = auditLog._id.reportID.airpotInfoID.quarter
    val start = new LocalDate(year, 1 + 3* (quarter-1), 1).toLocalDateTime(LocalTime.MIDNIGHT)
    val end = start.plusMonths(3).minusSeconds(1)
    val mntNum = auditLog._id.mntNum
    def fillSecAuditReport(): Unit ={
      val sheet = wb.getSheetAt(0)
      // Just take first 500
      val logs = auditLog.logs.filter(p=>p.dataType == AuditLog.DataTypeNoiseSec).sortBy(f=>f.time)
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
      val logs = auditLog.logs.filter(p=>p.dataType == AuditLog.DataTypeNoiseEvent).take(500).sortBy(_.time)
      sheet.getRow(0).getCell(0)
        .setCellValue(s"稽核資料：${terminalMap(mntNum)}事件監測資料")

      sheet.getRow(1).getCell(0)
        .setCellValue(s"稽核時間：${start.toString("yyyy/MM/dd")}到${end.toString("yyyy/MM/dd")} 23:59:59的事件監測資料")

      for((log, idx)<- logs.zipWithIndex){
        val row = sheet.createRow(4 + idx)
        var cell = row.createCell(0)
        val dt = new DateTime(log.time)
        cell.setCellValue(dt.toString("YYYY/MM/dd HH:mm:ss"))
        cell = row.createCell(1)
        cell.setCellValue(log.msg)
      }
    }

    def fillNoiseHourReport(): Unit ={
      val sheet = wb.getSheetAt(2)
      val logs = auditLog.logs.filter(p=>p.dataType == AuditLog.DataTypeNoiseHour).sortBy(_.time)
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
      val logs = auditLog.logs.filter(p=>p.dataType == AuditLog.DataTypeNoiseDay).sortBy(_.time)
      sheet.getRow(0).getCell(0)
        .setCellValue(s"稽核資料：${terminalMap(mntNum)}每日噪音監測資料")

      sheet.getRow(1).getCell(0)
        .setCellValue(s"稽核時間：${start.toString("yyyy/MM/dd")}到${end.toString("yyyy/MM/dd")} 23:59:59 每日資料")

      for((log, idx)<- logs.zipWithIndex){
        val row = sheet.createRow(16 + idx)
        var cell = row.createCell(0)
        val dt = new DateTime(log.time)
        cell.setCellValue(dt.toString("YYYY/MM/dd"))
        cell = row.createCell(1)
        cell.setCellValue(log.msg)
      }
    }


    def fillPeriodReport(sheetIndex:Int, tag:String): Unit ={
      val sheet = wb.getSheetAt(sheetIndex)
      val logs = auditLog.logs.filter(p=>p.dataType == tag).sortBy(_.time)
      sheet.getRow(0).getCell(0)
        .setCellValue(s"稽核資料：${terminalMap(mntNum)}$tag")

      sheet.getRow(1).getCell(0)
        .setCellValue(s"稽核時間：${start.toString("yyyy/MM/dd")}到${end.toString("yyyy/MM/dd")} 23:59:59 $tag")

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
    fillPeriodReport(3, AuditLog.DataTypeNoiseDay)
    fillPeriodReport(4, AuditLog.DataTypeNoiseMonth)
    fillPeriodReport(5, AuditLog.DataTypeNoiseQuarter)
    fillPeriodReport(6, AuditLog.DataTypeNoiseYear)

    def getProperFileName(name:String) = {
      name.replace("^\\.+", "").replaceAll("[\\\\/:*?\"<>|]", "")
    }

    val result =finishExcel(reportFilePath, pkg, wb)
    val properName = getProperFileName(s"${terminalMap(mntNum)}.xlsx")

    val targetFile = new File(result.toPath.getParent + s"/$properName")

    if(!result.renameTo(targetFile)) {
      Logger.error(s"faile to rename to ${targetFile}")
      result
    }else
      targetFile
  }

  def getImportErrorLog(logs: Seq[ImportErrorLog]): File ={
    val (reportFilePath, pkg, wb) = prepareTemplate("importErrorLog.xlsx")
    val evaluator = wb.getCreationHelper().createFormulaEvaluator()
    val format = wb.createDataFormat();
    val sheet = wb.getSheetAt(0)

    var rowNum = 1
    for(log<-logs){
      for(entry<-log.logs){
        val row = sheet.createRow(rowNum)
        row.createCell(0).setCellValue(entry.fileName)
        row.createCell(1).setCellValue(entry.terminal)
        row.createCell(2).setCellValue(entry.time)
        row.createCell(3).setCellValue(entry.dataType)
        row.createCell(4).setCellValue(entry.fieldName)
        row.createCell(5).setCellValue(entry.errorInfo)
        row.createCell(5).setCellValue(entry.value)
        rowNum = rowNum + 1
      }
    }

    finishExcel(reportFilePath, pkg, wb)
  }
}