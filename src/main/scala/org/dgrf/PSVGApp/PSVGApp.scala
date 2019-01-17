package org.dgrf.PSVGApp

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.dgrf.psvg.PSVG

import scala.io.Source

object PSVGMain {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("bheua").master("local").getOrCreate()

    val sqlContext = sparkSession.sqlContext
    //sqlContext.setConf("spark.sql.shuffle.partitions", "100")
    val inputXYfile = "/home/dgrfi/MEGA/DGRFFractal/testdata/samplexysm.csv"
    //val inputUniformfile = "/home/dgrfi/MEGA/DGRFFractal/testdata/tanpurasm.csv"
    val inputUniformfile = args(0)
    val outDegreeDistFile = "/home/dgrfi/MEGA/DGRFFractal/testdata/vgdegreeDist"
    val PSVGParamsFile = "/home/dgrfi/MEGA/DGRFFractal/testdata/psvgparm.json"

    println("gheu")
//    val psvgParamObj = PSVGUtil.readParametersFromJson(PSVGParamsFile)
    val PSVG = new PSVG(sparkSession)
    val psvgResults =PSVG.readXYTimeSeries(inputXYfile).createVisibilityGraph().calculateDegreeDistribution().savePSVGResults("PSVGResults",true)
    //val psvgResults = PSVG.readUniformTimeSeries(inputUniformfile).createVisibilityGraph()
    //val psvgResults = PSVG.readUniformTimeSeries(inputUniformfile).createVisibilityGraph().calculateDegreeDistribution(psvgParamObj).savePSVGResults("PSVGResults",true)
    //PSVG.readUniformTimeSeries(inputUniformfile).saveAdjacencyList("vgadjlist",true)
    //val psvgResults = PSVG.readUniformTimeSeries(inputUniformfile).createVisibilityGraph().calculateDegreeDistribution().getPSVGResults()

    println(psvgResults.PSVGFD,psvgResults.PSVGIntercept)

    //println("Fractal dimension "+fd)

  }


}
