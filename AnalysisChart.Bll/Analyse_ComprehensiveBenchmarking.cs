using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using AnalysisChart.Model;
using AnalysisChart.DalFactory;
using AnalysisChart.IDal;
namespace AnalysisChart.Bll
{
    public class Analyse_ComprehensiveBenchmarking
    {
        private const string Clinker = "熟料";
        private const string Cementmill = "水泥磨";
        private const string ElectricityConsumption = "ElectricityConsumption";
        private const string CoalConsumption = "CoalConsumption";
        private const string EnergyConsumption = "EnergyConsumption";
        private const string StaticsCycleDay = "day";
        private const string StaticsCycleMonth = "month";
        
        private static readonly IAnalyse_ComprehensiveBenchmarking dal_IComprehensiveBenchmarking = DalFactory.DalFactory.GetComprehensiveBenchmarkingInstance();
        public static string GetComprehensiveStaticsItems(string myOrganizationType, string myValueType, bool myHiddenMainMachine, List<string> myOrganizations)
        {
            DataTable m_StaticsItems = dal_IComprehensiveBenchmarking.GetComprehensiveStaticsItems(myOrganizationType, myValueType, myHiddenMainMachine, myOrganizations);
            return EasyUIJsonParser.TreeJsonParser.DataTableToJsonByLevelCode(m_StaticsItems, "LevelCode", "Name", new string[] { "OrganizationId", "TagColumnName", "TagTableName", "TagDataBase", "Type" });
        }
        public static DataTable GetMonthComprehensiveStaticsData(string myOrganizationId, string myVariableId, string myStatisticType, string myStartTime, string myEndTime)
        {
            DataTable m_MonthDataTable = new DataTable("DataValue");
            m_MonthDataTable.Columns.Add("Vdate",typeof(string));
            m_MonthDataTable.Columns.Add("TagValue", typeof(decimal));
            string m_LevelCode = Analyse_KPICommon.GetLevelCodeByOrganizationId(myOrganizationId);
            DateTime m_StartDate = DateTime.Parse(myStartTime);
            DateTime m_EndDate = DateTime.Parse(myEndTime);
            while (m_StartDate < m_EndDate )
            {
                if (myStatisticType == "Comprehensive")
                {
                    m_MonthDataTable.Rows.Add(m_StartDate.ToString("MM"), Analyse_KPICommon.GetComprehensiveConsumptionData(myVariableId, m_StartDate.ToString("yyyy-MM"), m_StartDate.ToString("yyyy-MM"), m_LevelCode, StaticsCycleMonth));
                }
                else if (myStatisticType == "Comparable")
                {
                    m_MonthDataTable.Rows.Add(m_StartDate.ToString("MM"), Analyse_KPICommon.GetComparableConsumptionData(myVariableId, m_StartDate.ToString("yyyy-MM"), m_StartDate.ToString("yyyy-MM"), m_LevelCode, StaticsCycleMonth));
                }
                m_StartDate = m_StartDate.AddMonths(1);
            }
            return m_MonthDataTable;
        }
        public static DataTable GetDayComprehensiveStaticsData(string myOrganizationId, string myVariableId, string myStatisticType, string myStartTime, string myEndTime)
        {
            DataTable m_DayDataTable = new DataTable("DataValue");
            m_DayDataTable.Columns.Add("Vdate", typeof(string));
            m_DayDataTable.Columns.Add("TagValue", typeof(decimal));
            string m_LevelCode = Analyse_KPICommon.GetLevelCodeByOrganizationId(myOrganizationId);
            DateTime m_StartDate = DateTime.Parse(myStartTime);
            DateTime m_EndDate = DateTime.Parse(myEndTime);
            while (m_StartDate < m_EndDate)
            {
                if (myStatisticType == "Comprehensive")
                {
                    m_DayDataTable.Rows.Add(m_StartDate.ToString("MMdd"), Analyse_KPICommon.GetComprehensiveConsumptionData(myVariableId, m_StartDate.ToString("yyyy-MM-dd"), m_StartDate.ToString("yyyy-MM-dd"), m_LevelCode, StaticsCycleDay));
                }
                else if (myStatisticType == "Comparable")
                {
                    m_DayDataTable.Rows.Add(m_StartDate.ToString("MMdd"), Analyse_KPICommon.GetComparableConsumptionData(myVariableId, m_StartDate.ToString("yyyy-MM-dd"), m_StartDate.ToString("yyyy-MM-dd"), m_LevelCode, StaticsCycleDay));
                }
                m_StartDate = m_StartDate.AddDays(1);
            }
            return m_DayDataTable;
        }
    }
}
