using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisChart.IDal;
using System.Configuration;
namespace AnalysisChart.DalFactory
{
    public class DalFactory
    {
        /// <summary>
        /// 获取对象
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private static object GetDataObject(string m_Type)
        {
            return DataFactory.DataFactory.GetObject(m_Type);
        }
        /// <summary>
        /// 产生一个实例
        /// </summary>
        /// <returns>返回实例类型</returns>
        public static IHistoryTrend GetHistoryTrendInstance()
        {
            string m_Type = ConfigurationManager.AppSettings["AnalysisChart"];
            IHistoryTrend m_IHistoryTrendObj = (IHistoryTrend)GetDataObject(m_Type + ".HistoryTrend");
            //m_IHistoryTrendObj.InitializeDbConn();
            return m_IHistoryTrendObj;
        }
        /// <summary>
        /// 产生一个实例
        /// </summary>
        /// <returns>返回实例类型</returns>
        public static IEnergyPredit GetEnergyPreditInstance()
        {
            string m_Type = ConfigurationManager.AppSettings["AnalysisChart"];
            IEnergyPredit m_IEnergyPreditObj = (IEnergyPredit)GetDataObject(m_Type + ".EnergyPredit");
            //m_IHistoryTrendObj.InitializeDbConn();
            return m_IEnergyPreditObj;
        }
        /// <summary>
        /// 产生一个实例
        /// </summary>
        /// <returns>返回实例类型</returns>
        public static IAnalyse_BenchmarkingBySameType GetBenchmarkingBySameType()
        {
            string m_Type = ConfigurationManager.AppSettings["AnalysisChart"];
            IAnalyse_BenchmarkingBySameType m_IAnalyse_BenchmarkingBySameTypeObj = (IAnalyse_BenchmarkingBySameType)GetDataObject(m_Type + ".Analyse_BenchmarkingBySameType");
            //m_IHistoryTrendObj.InitializeDbConn();
            return m_IAnalyse_BenchmarkingBySameTypeObj;
        }
        /// <summary>
        /// 产生一个实例,KPI对标公共方法
        /// </summary>
        /// <returns>返回实例类型</returns>
        public static IAnalyse_KPICommon GetKPICommonInstance()
        {
            string m_Type = ConfigurationManager.AppSettings["AnalysisChart"];
            IAnalyse_KPICommon m_IAnalyse_KPICommonObj = (IAnalyse_KPICommon)GetDataObject(m_Type + ".Analyse_KPICommon");
            //m_IHistoryTrendObj.InitializeDbConn();
            return m_IAnalyse_KPICommonObj;
        }
        /// <summary>
        /// 产生一个实例,工序电耗对标
        /// </summary>
        /// <returns>返回实例类型</returns>
        public static IAnalysisKPI_EntityBenchmarking GetEntityBenchmarkingInstance()
        {
            string m_Type = ConfigurationManager.AppSettings["AnalysisChart"];
            IAnalysisKPI_EntityBenchmarking m_IAnalysisKPI_EntityBenchmarkingObj = (IAnalysisKPI_EntityBenchmarking)GetDataObject(m_Type + ".AnalysisKPI_EntityBenchmarking");
            //m_IHistoryTrendObj.InitializeDbConn();
            return m_IAnalysisKPI_EntityBenchmarkingObj;
        }
        /// <summary>
        /// 产生一个实例,综合电耗对标
        /// </summary>
        /// <returns>返回实例类型</returns>
        public static IAnalyse_ComprehensiveBenchmarking GetComprehensiveBenchmarkingInstance()
        {
            string m_Type = ConfigurationManager.AppSettings["AnalysisChart"];
            IAnalyse_ComprehensiveBenchmarking m_IAnalyse_ComprehensiveBenchmarkingObj = (IAnalyse_ComprehensiveBenchmarking)GetDataObject(m_Type + ".Analyse_ComprehensiveBenchmarking");
            //m_IHistoryTrendObj.InitializeDbConn();
            return m_IAnalyse_ComprehensiveBenchmarkingObj;
        }
        /// <summary>
        /// 产生一个实例,生产指标横比对标
        /// </summary>
        /// <returns>返回实例类型</returns>
        public static IAnalyse_ProductionHorizontalComparison GetProductionHorizontalComparisonInstance()
        {
            string m_Type = ConfigurationManager.AppSettings["AnalysisChart"];
            IAnalyse_ProductionHorizontalComparison m_Analyse_ProductionHorizontalComparisonObj = (IAnalyse_ProductionHorizontalComparison)GetDataObject(m_Type + ".Analyse_ProductionHorizontalComparison");
            //m_IHistoryTrendObj.InitializeDbConn();
            return m_Analyse_ProductionHorizontalComparisonObj;
        }
        public static IAnalyse_QuantityDataWithQuery GetQuantityDataWithQueryInstance()
        {
            string m_Type = ConfigurationManager.AppSettings["AnalysisChart"];
            IAnalyse_QuantityDataWithQuery m_Analyse_QuantityDataWithQueryObj = (IAnalyse_QuantityDataWithQuery)GetDataObject(m_Type + ".Analyse_QuantityDataWithQuery");
            //m_IHistoryTrendObj.InitializeDbConn();
            return m_Analyse_QuantityDataWithQueryObj;
        }
    }
}
