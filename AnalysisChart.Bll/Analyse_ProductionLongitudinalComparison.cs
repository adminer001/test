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
    public class Analyse_ProductionLongitudinalComparison
    {
        private const string AnalyseCycYear = "year";
        private const string AnalyseCycMonth = "month";
        private const string AnalyseCycCustomDefine = "CustomDefine";

        private const string ComparableIndex = "ComparableIndex";
        private const string ComparableStandard = "ComparableStandard";
        private const string DcsData = "DcsData";
        private const string EntityProductionStatics = "EntityProductionStatics"; 
        //private const string EntityBenchmarkingStatics = "EntityBenchmarkingStatics";              //工序指标
        //private const string ComprehensiveStatics = "ComprehensiveStatics";                      //综合指标
        private const string CustomDefine = "CustomDefine";

        private const string ContrastTableName = "DCSContrast";
        private const string HistoryDCSIncreasement = "HistoryDCSIncreasement";

        private const string TimeOfLastCycName = "同期";

        private const string StaticsCycleDay = "day";
        private const string StaticsCycleMonth = "month";
        private static readonly IAnalyse_KPICommon dal_IAnalyse_KPICommon = DalFactory.DalFactory.GetKPICommonInstance();
        private static readonly IHistoryTrend dal_HistoryTrend = DalFactory.DalFactory.GetHistoryTrendInstance();

        public static string GetChartDataJson(string myAnalyseCyc, string myStartTime, string myEndTime, string myChartType, string myTagInfoJson)
        {
            SqlServerDataAdapter.SqlServerDataFactory m_SqlServerDataAdapter = new SqlServerDataAdapter.SqlServerDataFactory(GetDbConnectionString("ConnNXJC"));
            string m_StartTime = "";
            string m_EndTime = "";
            string m_LastCycStartTime = "";
            string m_LastCycEndTime = "";
            string m_TagItemId = "";
            string m_TagField = "";
            string m_TagTable = "";
            string m_TagDataBase = "";
            string m_TagTabClass = "";               //前台TabId
            string m_UnitX = "";
            string m_UnitY = "";
            string m_StaticsCycleType = "";
            DataTable m_ChartDataTableStruct;
            try
            {
                if (myAnalyseCyc == AnalyseCycYear)                //按月统计
                {
                    m_UnitX = "月份";
                    DateTime m_DateTime = DateTime.Parse(myStartTime);
                    m_StartTime = m_DateTime.ToString("yyyy") + "-01-01";
                    m_EndTime = m_DateTime.ToString("yyyy") + "-12-31";

                    m_LastCycStartTime = m_DateTime.AddYears(-1).ToString("yyyy") + "-01-01";
                    m_LastCycEndTime = m_DateTime.AddYears(-1).ToString("yyyy") + "-12-31";

                    m_StaticsCycleType = StaticsCycleMonth;
                }
                else if (myAnalyseCyc == AnalyseCycMonth)          //按日统计
                {
                    m_UnitX = "天";
                    DateTime m_DateTime = DateTime.Parse(myStartTime);
                    m_StartTime = m_DateTime.ToString("yyyy-MM") + "-01";
                    m_EndTime = DateTime.Parse(m_StartTime).AddMonths(1).AddDays(-1).ToString("yyyy-MM-dd");

                    m_LastCycStartTime = m_DateTime.AddYears(-1).ToString("yyyy-MM") + "-01";
                    m_LastCycEndTime = DateTime.Parse(m_LastCycStartTime).AddMonths(1).AddDays(-1).ToString("yyyy-MM-dd");

                    m_StaticsCycleType = StaticsCycleDay;
                }
                else if (myAnalyseCyc == AnalyseCycCustomDefine)   //用户自定义
                {
                    DateTime m_StartDateTime = DateTime.Parse(myStartTime);
                    DateTime m_EndDateTime = DateTime.Parse(myEndTime);
                    m_UnitX = "天";
                    m_StartTime = myStartTime;
                    m_EndTime = m_EndDateTime.ToString("yyyy-MM-dd");


                    m_LastCycStartTime = m_StartDateTime.AddYears(-1).ToString("yyyy-MM-dd");
                    m_LastCycEndTime = m_EndDateTime.AddYears(-1).ToString("yyyy-MM-dd");

                    m_StaticsCycleType = StaticsCycleDay;
                }

                List<string> m_ColumnNameList = new List<string>();
                //////创建报表结构
                m_ChartDataTableStruct = CreateChartDataTableStruct(myAnalyseCyc, m_StartTime, m_EndTime, m_ColumnNameList);     //生成存储数据的DataTable

                string[] m_RowsJson = EasyUIJsonParser.Infrastructure.JsonHelper.ArrayPicker("rows", myTagInfoJson);
                DataTable m_TagsInfoTable = EasyUIJsonParser.DataGridJsonParser.JsonToDataTable(m_RowsJson, GetTagsInfoTable());
                foreach (DataRow m_DataRow in m_TagsInfoTable.Rows)
                {
                    m_TagItemId = m_DataRow["TagItemId"].ToString();             //指标名称
                    m_TagField = m_DataRow["TagId"].ToString();                   //设备id
                    m_TagTable = m_DataRow["TagTable"].ToString();
                    m_TagDataBase = m_DataRow["TagDataBase"].ToString();
                    m_TagTabClass = m_DataRow["TagTabClass"].ToString();
                    string m_OtherInfo = m_DataRow["OtherInfo"].ToString();


                    if (m_TagTabClass == ComparableStandard)               //直接读数情况
                    {
                        string[] m_ValueGroup = GetComparableStandardData(m_OtherInfo, m_ChartDataTableStruct);
                        m_ChartDataTableStruct.Rows.Add(m_ValueGroup);
                    }
                    else if (m_TagTabClass == DcsData)                       //从数据库表中直接查询值
                    {
                        m_TagField = m_DataRow["TagId"].ToString();  
                        m_TagTable = m_DataRow["TagTable"].ToString();
                        m_TagDataBase = m_DataRow["TagDataBase"].ToString();
                        DataTable m_TagValues = null;
                        if (TimeOfLastCycName == m_DataRow["SameTimeOfLastCyc"].ToString())        //如果是去年同期
                        {
                            m_TagValues = GetDcsDataTable(myAnalyseCyc, m_LastCycStartTime, m_LastCycEndTime, m_TagField, m_TagTable, m_TagDataBase);
                        }
                        else
                        {
                            m_TagValues = GetDcsDataTable(myAnalyseCyc, m_StartTime, m_EndTime, m_TagField, m_TagTable, m_TagDataBase);
                        }

                        if (m_TagValues != null)
                        {
                            string[] m_DataColumnsItem = GetDataColumnsItem(m_TagValues, m_ChartDataTableStruct);
                            m_ChartDataTableStruct.Rows.Add(m_DataColumnsItem);
                        }
                    }
                    else if (m_TagTabClass == EntityProductionStatics)               //工序指标
                    {
                        string m_VaribleId = m_DataRow["TagId"].ToString();
                        //string m_OrganizationId = m_DataRow["TagTable"].ToString();
                        ///////////////获得标签数据
                        DataTable m_TagValues = null;
                        if (TimeOfLastCycName == m_DataRow["SameTimeOfLastCyc"].ToString())        //如果是去年同期
                        {
                            string m_FactoryOrganzationId = GetFactoryOrganzationId(m_DataRow["TagTable"].ToString(), m_SqlServerDataAdapter);
                            m_TagValues = RunIndicators.EquipmentRunIndicators.GetEquipmentUtilizationPerMonth(m_TagItemId, m_FactoryOrganzationId, m_VaribleId, m_LastCycStartTime, m_LastCycEndTime, m_SqlServerDataAdapter);
                        }
                        else
                        {
                            string m_FactoryOrganzationId = GetFactoryOrganzationId(m_DataRow["TagTable"].ToString(), m_SqlServerDataAdapter);
                            m_TagValues = RunIndicators.EquipmentRunIndicators.GetEquipmentUtilizationPerMonth(m_TagItemId, m_FactoryOrganzationId, m_VaribleId, m_StartTime, m_EndTime, m_SqlServerDataAdapter);
                        }
                        if (m_TagValues != null)
                        {
                            string[] m_DataColumnsItem = GetDataColumnsItem(m_TagValues, m_ChartDataTableStruct);
                            m_ChartDataTableStruct.Rows.Add(m_DataColumnsItem);
                        }
                    }
                    
                }

                /////////////////////////////获取趋势的行名称和列名称//////////////////////
                if (m_TagsInfoTable != null && m_TagsInfoTable != null && m_ChartDataTableStruct != null)
                {
                    string[] m_RowsName = new string[m_TagsInfoTable.Rows.Count];
                    for (int i = 0; i < m_TagsInfoTable.Rows.Count; i++)
                    {
                        if (TimeOfLastCycName == m_TagsInfoTable.Rows[i]["SameTimeOfLastCyc"].ToString())
                        {
                            m_RowsName[i] = "[" + m_TagsInfoTable.Rows[i]["SameTimeOfLastCyc"].ToString() + "]" + m_TagsInfoTable.Rows[i]["TagItemName"].ToString();
                        }
                        else
                        {
                            m_RowsName[i] = m_TagsInfoTable.Rows[i]["TagItemName"].ToString();
                        }

                    }
                    string m_ChartData = EasyUIJsonParser.ChartJsonParser.GetGridChartJsonString(m_ChartDataTableStruct, m_ColumnNameList.ToArray(), m_RowsName, m_UnitX, m_UnitY, 1);
                    return m_ChartData;
                }
                else
                {
                    return "{\"rows\":[],\"total\":0}";
                }
            }
            catch (Exception err)
            {
                return "{\"rows\":[],\"total\":0}";
            }
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
        /// <summary>
        /// 获得形成Chart的表结构
        /// </summary>
        /// <param name="myQueryGroupFlag">查询周期分组类型</param>
        /// <param name="myStartTime">开始时间</param>
        /// <param name="myEndTime">结束时间</param>
        /// <param name="myColumnName">字段名称</param>
        /// <returns>表结构</returns>
        private static DataTable CreateChartDataTableStruct(string myQueryGroupFlag, string myStartTime, string myEndTime, List<string> myColumnName)
        {
            //根据起始时间段判断所取的变量的间隔周期
            try
            {
                DateTime m_StartTime = DateTime.Parse(myStartTime);
                DateTime m_EndTime = DateTime.Parse(myEndTime);
                DataTable m_DataTable = new DataTable("ChartDataTable");
                if (myQueryGroupFlag == AnalyseCycYear) //年统计,按月
                {
                    for (int i = 1; i <= 12; i++)
                    {
                        //m_DataTable.Columns.Add(m_StartTime.ToString("yyyy") + i.ToString("d2"), typeof(decimal));
                        m_DataTable.Columns.Add(i.ToString("d2"), typeof(decimal));
                        myColumnName.Add(i.ToString() + "月");
                    }
                }
                else if (myQueryGroupFlag == AnalyseCycMonth)             //月统计,按日
                {
                    DateTime m_DateTime = DateTime.Parse(myStartTime);
                    int m_ColumnCount = DateTime.DaysInMonth(m_DateTime.Year, m_DateTime.Month);
                    for (int i = 1; i <= m_ColumnCount; i++)
                    {
                        //m_DataTable.Columns.Add(m_StartTime.ToString("yyyyMM") + i.ToString("d2"), typeof(decimal));
                        m_DataTable.Columns.Add(m_StartTime.ToString("MM") + i.ToString("d2"), typeof(decimal));
                        myColumnName.Add(i.ToString());
                    }
                }
                else if (myQueryGroupFlag == AnalyseCycCustomDefine)      //自定义
                {
                    DateTime m_StartDateTime = DateTime.Parse(myStartTime);
                    DateTime m_EndDateTime = DateTime.Parse(myEndTime);
                    while (m_StartDateTime <= m_EndDateTime)
                    {
                        //m_DataTable.Columns.Add(m_StartDateTime.ToString("yyyyMMdd"), typeof(decimal));
                        m_DataTable.Columns.Add(m_StartDateTime.ToString("MMdd"), typeof(decimal));
                        myColumnName.Add(m_StartDateTime.ToString("MM-dd"));
                        m_StartDateTime = m_StartDateTime.AddDays(1);
                    }
                }
                return m_DataTable;
            }
            catch
            {
                return null;
            }
        }
        private static DataTable GetTagsInfoTable()
        {
            DataTable m_TagsInfoTable = new DataTable("TagsInfoTable");
            m_TagsInfoTable.Columns.Add("TagItemId", typeof(string));
            m_TagsInfoTable.Columns.Add("TagItemName", typeof(string));
            m_TagsInfoTable.Columns.Add("TagId", typeof(string));
            m_TagsInfoTable.Columns.Add("TagStaticsType", typeof(string));
            m_TagsInfoTable.Columns.Add("TagTable", typeof(string));
            m_TagsInfoTable.Columns.Add("TagDataBase", typeof(string));
            m_TagsInfoTable.Columns.Add("TagDescription", typeof(string));
            m_TagsInfoTable.Columns.Add("TagTabClass", typeof(string));
            m_TagsInfoTable.Columns.Add("SameTimeOfLastCyc", typeof(string));
            m_TagsInfoTable.Columns.Add("OtherInfo", typeof(string));
            return m_TagsInfoTable;
        }
        private static string[] GetDataColumnsItem(DataTable myTagValues, DataTable myChartDataTableStruct)
        {
            int m_TagValuesIndex = 0;
            string[] m_DataColumnsItem = new string[myChartDataTableStruct.Columns.Count];    //创建一行
            for (int j = 0; j < myChartDataTableStruct.Columns.Count; j++)
            {
                m_DataColumnsItem[j] = "0";
                m_TagValuesIndex = 0;
                while (m_TagValuesIndex < myTagValues.Columns.Count)
                {
                    string m_StructColumnName = myChartDataTableStruct.Columns[j].ColumnName;
                    string m_ValueColumnName = myTagValues.Columns[m_TagValuesIndex].ColumnName.Substring(myTagValues.Columns[m_TagValuesIndex].ColumnName.Length - 2);
                    if (m_StructColumnName == m_ValueColumnName)
                    {
                        m_DataColumnsItem[j] = myTagValues.Rows[0][myTagValues.Columns[m_TagValuesIndex].ColumnName].ToString();
                        break;
                    }
                    m_TagValuesIndex = m_TagValuesIndex + 1;
                }
            }
            return m_DataColumnsItem;
        }
        private static string[] GetComparableStandardData(string myOtherInfo, DataTable myChartDataTableStruct)
        {
            decimal m_ValueData = 0.0m;
            try
            {
                m_ValueData = decimal.Parse(myOtherInfo);
            }
            catch
            {
                m_ValueData = 0.0m;
            }
            string[] m_ValueGroup = new string[myChartDataTableStruct.Columns.Count];
            for (int i = 0; i < myChartDataTableStruct.Columns.Count; i++)
            {
                try
                {
                    m_ValueGroup[i] = m_ValueData.ToString();

                }
                catch
                {

                }
            }
            return m_ValueGroup;
        }
        /// <summary>
        /// 获得DCS数据
        /// </summary>
        /// <param name="myAnalyseCyc"></param>
        /// <param name="myStartTime"></param>
        /// <param name="myEndTime"></param>
        /// <param name="myTagField"></param>
        /// <param name="myTagTable"></param>
        /// <param name="myTagDataBase"></param>
        /// <returns></returns>
        private static DataTable GetDcsDataTable(string myAnalyseCyc, string myStartTime, string myEndTime, string myTagField, string myTagTable, string myTagDataBase)
        {
            DataTable m_TagItemInfoTable = dal_HistoryTrend.GetTagInfoByVariableName(myTagDataBase, ContrastTableName, myTagField);
            if (m_TagItemInfoTable != null && m_TagItemInfoTable.Rows.Count > 0)
            {
                string m_DataBaseName = "";
                string m_DataTableName = "";
                string m_DataFieldName = "";
                bool m_IsCumulant = (bool)m_TagItemInfoTable.Rows[0]["IsCumulant"];
                if (m_IsCumulant == true)         //如果是增量,则在增量表中取数据
                {
                    m_DataBaseName = m_TagItemInfoTable.Rows[0]["CumulantDataBase"].ToString();
                    m_DataTableName = HistoryDCSIncreasement;
                    m_DataFieldName = m_TagItemInfoTable.Rows[0]["CumulantName"].ToString();

                }
                else
                {
                    m_DataBaseName = myTagDataBase;
                    m_DataTableName = "History_" + m_TagItemInfoTable.Rows[0]["TableName"].ToString();
                    m_DataFieldName = m_TagItemInfoTable.Rows[0]["FieldName"].ToString();
                }

                ///////////////获得标签数据
                if (myAnalyseCyc == AnalyseCycYear)                //按月统计
                {
                    DataTable m_TagValues = dal_IAnalyse_KPICommon.GetMonthDCSData(m_DataBaseName, m_DataTableName, m_DataFieldName, myStartTime, myEndTime, m_IsCumulant);
                    return m_TagValues;
                }
                else if (myAnalyseCyc == AnalyseCycMonth || myAnalyseCyc == AnalyseCycCustomDefine)          //按日统计
                {
                    DataTable m_TagValues = dal_IAnalyse_KPICommon.GetDayDCSData(m_DataBaseName, m_DataTableName, m_DataFieldName, myStartTime, myEndTime, m_IsCumulant);
                    return m_TagValues;
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
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
