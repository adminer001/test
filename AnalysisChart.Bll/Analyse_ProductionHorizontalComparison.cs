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
    public class Analyse_ProductionHorizontalComparison
    {
        private const string StaticsCycleDay = "day";
        private const string StaticsCycleMonth = "month";
        private static readonly IAnalyse_ProductionHorizontalComparison dal_IAnalyse_ProductionHorizontalComparison = DalFactory.DalFactory.GetProductionHorizontalComparisonInstance();
        public static string GetIndicatorItems()
        {
            DataTable m_RunIndicatorsItemsTable = RunIndicators.RunIndicatorsItems.GetRunIndicatorsItemsTable();
            DataRow[] m_RunIndicatorsRows = m_RunIndicatorsItemsTable.Select("IndicatorType in ('MaterialWeight','EquipmentUtilization')");
            string ValueString = EasyUIJsonParser.TreeJsonParser.DataTableToJsonByLevelCodeWithIdColumn(m_RunIndicatorsRows.CopyToDataTable(), "LevelCode", "IndicatorId", "IndicatorName");
            return ValueString;
        }
        public static string GetEquipmentInfo(List<string> myDataValidIdGroup)
        {
            DataTable m_EquipmentInfoTable = dal_IAnalyse_ProductionHorizontalComparison.GetEquipmentInfo(myDataValidIdGroup);
            string m_ValueString = EasyUIJsonParser.DataGridJsonParser.DataTableToJson(m_EquipmentInfoTable);
            return m_ValueString;
        }
        public static string GetStaticsItems(string myValueType, string myEquipmentCommonId, string mySpecificationsId, List<string> myOrganizations)
        {
            DataTable m_StaticsItemsTable = dal_IAnalyse_ProductionHorizontalComparison.GetStaticsItems(myValueType, myEquipmentCommonId, mySpecificationsId, myOrganizations);
            string m_ValueString = EasyUIJsonParser.TreeJsonParser.DataTableToJsonByLevelCodeWithIdColumn(m_StaticsItemsTable, "LevelCode", "EquipmentId", "Name", new string[] { "OrganizationId", "LevelType", "LevelCode", "EquipmentCommonId" });
            return m_ValueString;
        }
        public static string GetHorizontalComparisonDataValue(string myStartTime, string myEndTime, string myTagInfoJson)
        {
            string m_StartTime = myStartTime + "-01";
            string m_EndTime = DateTime.Parse(myEndTime + "-01").AddMonths(1).AddDays(-1).ToString("yyyy-MM-dd");
            SqlServerDataAdapter.SqlServerDataFactory m_SqlServerDataAdapter = new SqlServerDataAdapter.SqlServerDataFactory(GetDbConnectionString("ConnNXJC"));
            string m_ValueJson = "";
            string[] m_RowsJson = EasyUIJsonParser.Infrastructure.JsonHelper.ArrayPicker("rows", myTagInfoJson);
            DataTable m_TagsInfoTable = EasyUIJsonParser.DataGridJsonParser.JsonToDataTable(m_RowsJson, GetTagsInfoTable());
            if (m_TagsInfoTable != null && m_TagsInfoTable.Rows.Count > 0)
            {
                List<string> m_OrganizationList = new List<string>(0);
                List<string> m_VariableList = new List<string>(0);
                foreach (DataRow item in m_TagsInfoTable.Rows)           //生产运行指标
                {
                    if (item["OrganizationId"] != null && item["OrganizationId"].ToString() != "")
                    {
                        if (item["StatisticType"].ToString() == "Entity")     //生产运行指标
                        {
                            string m_FactoryOrganzationId = GetFactoryOrganzationId(item["OrganizationId"].ToString(), m_SqlServerDataAdapter);
                            item["Value"] = string.Format("{0:F2}", RunIndicators.EquipmentRunIndicators.GetEquipmentUtilization(item["TagItemId"].ToString(), item["VariableId"].ToString(), m_FactoryOrganzationId, m_StartTime, m_EndTime, m_SqlServerDataAdapter));
                        }
                    }
                }
                m_ValueJson = CreateChartString(m_TagsInfoTable, m_TagsInfoTable.Rows[0]["StatisticName"].ToString(), m_TagsInfoTable.Rows[0]["StatisticType"].ToString());
            }
            return m_ValueJson;
        }
        private static string GetFactoryOrganzationId(string myOrganzationId, SqlServerDataAdapter.SqlServerDataFactory mySqlServerDataAdapter)
        {
            string m_Sql = @"select E.OrganizationID as OrganizationId,E.LevelCode as FactoryLevelCode,F.LevelCode as ParmeterLevelCode from system_Organization E, system_Organization F
                                where F.OrganizationID = '{0}'
                                and CHARINDEX(E.LevelCode, F.LevelCode) > 0
                                and E.LevelType = 'Factory'";
            m_Sql = string.Format(m_Sql, myOrganzationId);

            DataTable m_FactoryOrganzationTable = mySqlServerDataAdapter.Query(m_Sql);
            if (m_FactoryOrganzationTable != null && m_FactoryOrganzationTable.Rows.Count > 0)
            {
                return m_FactoryOrganzationTable.Rows[0]["OrganizationId"].ToString();
            }
            else
            {
                return myOrganzationId;
            }

        }
        private static DataTable GetTagsInfoTable()
        {
            DataTable m_TagsInfoTable = new DataTable("TagsInfoTable");
            m_TagsInfoTable.Columns.Add("TagItemId", typeof(string));
            m_TagsInfoTable.Columns.Add("Name", typeof(string));
            m_TagsInfoTable.Columns.Add("OrganizationId", typeof(string));
            m_TagsInfoTable.Columns.Add("LevelCode", typeof(string));
            m_TagsInfoTable.Columns.Add("StatisticType", typeof(string));
            m_TagsInfoTable.Columns.Add("StatisticName", typeof(string));
            m_TagsInfoTable.Columns.Add("VariableId", typeof(string));
            m_TagsInfoTable.Columns.Add("Value", typeof(string));
            return m_TagsInfoTable;
        }
        private static string CreateChartString(DataTable myValueDataTable, string myValueTypeName, string myValueType)
        {
            List<string> m_ColumnNameList = new List<string>();
            DataTable m_ChartDataTableStruct = CreateChartDataTableStruct(myValueDataTable, ref m_ColumnNameList);
            string m_UnitY = "";
            string[] m_RowsName = new string[1];
            m_RowsName[0] = myValueTypeName;
            if (myValueType == "ElectricityConsumption_Comprehensive" || myValueType == "ElectricityConsumption_Comparable")
            {
                m_UnitY = "kW·h/t";
            }
            else
            {
                m_UnitY = "kgce/t";
            }
            string m_ChartData = EasyUIJsonParser.ChartJsonParser.GetGridChartJsonString(m_ChartDataTableStruct, m_ColumnNameList.ToArray(), m_RowsName, "", m_UnitY, 1);
            return m_ChartData;
        }
        private static DataTable CreateChartDataTableStruct(DataTable myValueDataTable, ref List<string> myColumnNameList)
        {
            DataTable m_DataTable = new DataTable("ChartDataTable");
            string[] m_ValueGroup = new string[myValueDataTable.Rows.Count];
            for (int i = 0; i < myValueDataTable.Rows.Count; i++)
            {
                m_DataTable.Columns.Add(myValueDataTable.Rows[i]["TagItemId"].ToString() + myValueDataTable.Rows[i]["VariableId"].ToString(), typeof(decimal));
                myColumnNameList.Add(myValueDataTable.Rows[i]["Name"].ToString());
                string m_Value = myValueDataTable.Rows[i]["Value"].ToString();
                m_ValueGroup[i] = m_Value == "" ? "0.0" : m_Value;
            }
            m_DataTable.Rows.Add(m_ValueGroup);
            return m_DataTable;
        }
        private static string GetDbConnectionString(string myKeyWord)
        {
            try
            {
                return System.Configuration.ConfigurationManager.ConnectionStrings[myKeyWord].ToString();
            }
            catch
            {
                return "";
            }
        }
    }
}
