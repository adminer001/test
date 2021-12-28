using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using AnalysisChart.Model;
using AnalysisChart.DalFactory;
using AnalysisChart.IDal;
using System.Data.SqlClient;
namespace AnalysisChart.Bll
{
    public class HistoryTrend
    {
        private const string YearsTable = "YearsTable";
        private const string MonthsTable = "MonthsTable";
        private const string DaysTable = "DaysTable";
        private const string HoursTable = "HoursTable";
        private const string MinutesTable = "MinutesTable";
        private const string ContrastTableName = "DCSContrast";
        private const string HistoryDCSIncreasement = "HistoryDCSIncreasement";
        public static SqlServerDataAdapter.SqlServerDataFactory m_SqlServerDataAdapter = new SqlServerDataAdapter.SqlServerDataFactory(GetDbConnectionString("ConnNXJC"));
        //获取主设备信息
        public static DataTable GetVariableInfoData()
        {
            string m_Sql = @"SELECT [KeyID],[EquipmentName]
                               FROM [dbo].[equipment_EquipmentDetail]";
            m_Sql = string.Format(m_Sql);

            DataTable equipmentInfo = m_SqlServerDataAdapter.Query(m_Sql);
            return equipmentInfo;
        }
        //获取主设备下对应的运行参数
        public static DataTable GetParamInfoTable(string variableId)
        {
            string m_SqlParam = @"SELECT [VariableName] as  TagItemId,
                                                [VariableDescription] as TagItemName
                                  FROM [dbo].[system_SlaveMachineDescription]
                                  where [KeyID] = @variableId";
            SqlParameter parameter = new SqlParameter("@variableId", variableId);
            m_SqlParam = string.Format(m_SqlParam);
            DataTable paramTable = m_SqlServerDataAdapter.Query(m_SqlParam, parameter);
            return paramTable;

        }
        //获取标签列表
        public static DataTable GetVariableTable(string m_Type)
        {
            m_Type = m_Type.Substring(0, m_Type.Length - 1);
            m_Type = m_Type.Replace(",", "','");
            m_Type = "'" + m_Type + "'";
            string mySql = @"SELECT [VariableName] as  TagItemId,
                                              [VariableDescription] as TagItemName
                                         FROM [zc_jxdcc_jxdcf].[dbo].[View_DCSContrast]
                                        where [VariableName] in (";
            mySql = mySql + m_Type + ")";
            //SqlParameter parameter = new SqlParameter("@m_Type", m_Type);
            DataTable paramTable = m_SqlServerDataAdapter.Query(mySql);
            return paramTable;

        }
        //基于接口实例化动态链接库
        private static readonly IHistoryTrend dal_HistoryTrend = DalFactory.DalFactory.GetHistoryTrendInstance();

        public static string GetOneChartDataJson(string myStartTime, string myEndTime, string myOneTagInfo)
        {
            string myOneTagStr = "";
            if (myOneTagInfo != "")
            {
                myOneTagInfo = myOneTagInfo.Substring(0, myOneTagInfo.Length - 1);
                myOneTagInfo = myOneTagInfo.Replace(",", "','");
                myOneTagInfo = "'" + myOneTagInfo + "'";
                string myOneTagSql = @"SELECT [DBName] as TagDataBase,[VariableName] as  TagItemId,
                                              [VariableDescription] as TagItemName,[TableName] as TagDataTable,[FieldName] as TagFieldId
                                         FROM [zc_jxdcc_jxdcf].[dbo].[View_DCSContrast]
                                        where [VariableName] in (";
                myOneTagSql = myOneTagSql + myOneTagInfo + ")";
                DataTable myOneTagTable = m_SqlServerDataAdapter.Query(myOneTagSql);
                myOneTagStr = GetChartDataJson(myStartTime, myEndTime, myOneTagTable);
            }
            return myOneTagStr;
        }
        public static string GetTwoChartDataJson(string myStartTime, string myEndTime, string myTwoTagInfo)
        {
            string myTwoTagStr = "";
            if (myTwoTagInfo != "")
            {
                myTwoTagInfo = myTwoTagInfo.Substring(0, myTwoTagInfo.Length - 1);
                myTwoTagInfo = myTwoTagInfo.Replace(",", "','");
                myTwoTagInfo = "'" + myTwoTagInfo + "'";
                string myTwoTagSql = @"SELECT [DBName] as TagDataBase,[VariableName] as  TagItemId,
                                              [VariableDescription] as TagItemName,[TableName] as TagDataTable,[FieldName] as TagFieldId
                                         FROM [zc_jxdcc_jxdcf].[dbo].[View_DCSContrast]
                                        where [VariableName] in(";
                myTwoTagSql = myTwoTagSql + myTwoTagInfo + ")";
                DataTable myTwoTagTable = m_SqlServerDataAdapter.Query(myTwoTagSql);
                myTwoTagStr = GetChartDataJson(myStartTime, myEndTime, myTwoTagTable);
            }
            return myTwoTagStr;
        }

        public static string GetChartDataJson(string myStartTime, string myEndTime, DataTable m_TagsInfoTable)
        {

            string[] m_RowsName;               //趋势的行名称
            List<string> m_ColumnNameList = new List<string>();            //趋势的列名称
            string m_UnitX = "";
            string m_UnitY = "";
            ////////////////////构造显示趋势的表结构/////////////////////
            DataTable m_TagTrendDataTable = GetDataTrendDataTable(myStartTime, myEndTime, m_ColumnNameList);
            if (m_TagsInfoTable != null && m_TagsInfoTable != null && m_TagTrendDataTable != null)
            {
                for (int i = 0; i < m_TagsInfoTable.Rows.Count; i++)
                {
                    try
                    {
                        DataTable m_TagValues = GetTagValues(m_TagTrendDataTable.TableName,
                                                             m_TagsInfoTable.Rows[i]["TagDataBase"].ToString(),
                                                             m_TagsInfoTable.Rows[i]["TagItemId"].ToString(),
                                                             myStartTime, myEndTime);
                        if (m_TagValues != null)
                        {
                            int m_TagValuesIndex = 0;
                            string[] m_DataColumnsItem = new string[m_TagTrendDataTable.Columns.Count];    //创建一行
                            for (int j = 0; j < m_TagTrendDataTable.Columns.Count; j++)
                            {
                                m_DataColumnsItem[j] = "0";
                                m_TagValuesIndex = 0;
                                while (m_TagValuesIndex < m_TagValues.Rows.Count)
                                {
                                    if (m_TagTrendDataTable.Columns[j].ColumnName == m_TagValues.Rows[m_TagValuesIndex]["VDate"].ToString())
                                    {
                                        m_DataColumnsItem[j] = m_TagValues.Rows[m_TagValuesIndex]["TagValue"].ToString();
                                        break;
                                    }
                                    m_TagValuesIndex = m_TagValuesIndex + 1;
                                }
                            }
                            m_TagTrendDataTable.Rows.Add(m_DataColumnsItem);
                        }
                    }
                    catch
                    {

                    }
                }
            }
            /////////////////////////////获取趋势的行名称和列名称//////////////////////
            if (m_TagsInfoTable != null && m_TagsInfoTable != null && m_TagTrendDataTable != null)
            {
                m_RowsName = new string[m_TagsInfoTable.Rows.Count];
                for (int i = 0; i < m_TagsInfoTable.Rows.Count; i++)
                {
                    m_RowsName[i] = m_TagsInfoTable.Rows[i]["TagItemName"].ToString();
                }
                m_UnitX = GetUnitX(m_TagTrendDataTable.TableName);
                string m_ChartData = EasyUIJsonParser.ChartJsonParser.GetGridChartJsonString(m_TagTrendDataTable, m_ColumnNameList.ToArray(), m_RowsName, m_UnitX, m_UnitY, 1);
                return m_ChartData;
            }
            else
            {
                return "\"rows\":[]";
            }
        }
        private static string GetUnitX(string myTagTrendDataTableName)
        {
            if (myTagTrendDataTableName == YearsTable)          //年趋势
            {
                return "年份";
            }
            else if (myTagTrendDataTableName == MonthsTable)          //月趋势
            {
                return "月份";
            }
            else if (myTagTrendDataTableName == DaysTable)          //日趋势
            {
                return "天";
            }
            else if (myTagTrendDataTableName == HoursTable)          //时趋势
            {
                return "小时";
            }
            else if (myTagTrendDataTableName == MinutesTable)          //分趋势
            {
                return "分钟";
            }
            else
            {
                return "";
            }
        }
        /// <summary>
        /// 前台返回的要显示趋势的标签列表
        /// </summary>
        /// <returns>表结构</returns>
        private static DataTable GetTagsInfoTable()
        {
            DataTable m_TagsInfoTable = new DataTable("TagsInfoTable");
            m_TagsInfoTable.Columns.Add("TagItemId", typeof(string));
            m_TagsInfoTable.Columns.Add("TagItemName", typeof(string));
            m_TagsInfoTable.Columns.Add("TagDataBase", typeof(string));
            m_TagsInfoTable.Columns.Add("TagDataTable", typeof(string));
            m_TagsInfoTable.Columns.Add("TagFieldId", typeof(string));
            return m_TagsInfoTable;
        }
        /// <summary>
        /// 获得趋势显示的表结构
        /// </summary>
        /// <param name="myStartTime">开始时间</param>
        /// <param name="myEndTime">结束时间</param>
        /// <param name="myColumnName">字段名列表</param>
        /// <returns>表结构</returns>
        private static DataTable GetDataTrendDataTable(string myStartTime, string myEndTime, List<string> myColumnName)
        {
            //根据起始时间段判断所取的变量的间隔周期
            try
            {
                DateTime m_StartTime = DateTime.Parse(myStartTime);
                DateTime m_EndTime = DateTime.Parse(myEndTime);

                long m_IntervalTime = Int64.Parse(m_EndTime.ToString("yyyyMMddHHmm")) - Int64.Parse(m_StartTime.ToString("yyyyMMddHHmm"));
                if (m_IntervalTime >= 1000000000)        //十年以上按年进行查找
                {
                    DataTable m_TagTrendData = new DataTable(YearsTable);

                    int m_AddYearIndex = 0;
                    while (m_StartTime.AddYears(m_AddYearIndex) <= m_EndTime)
                    {
                        m_TagTrendData.Columns.Add(DateTime.Parse(myStartTime).AddYears(m_AddYearIndex).ToString("yyyy"), typeof(decimal));
                        myColumnName.Add(DateTime.Parse(myStartTime).AddYears(m_AddYearIndex).ToString("yyyy年"));
                        m_AddYearIndex = m_AddYearIndex + 1;
                    }
                    return m_TagTrendData;
                }
                else if (m_IntervalTime < 1000000000 && m_IntervalTime >= 100000000)   //1年以上10年以下按照月进行查找
                {
                    DataTable m_TagTrendData = new DataTable(MonthsTable);

                    int m_AddMonthsIndex = 0;
                    while (m_StartTime.AddMonths(m_AddMonthsIndex) <= m_EndTime)
                    {
                        m_TagTrendData.Columns.Add(DateTime.Parse(myStartTime).AddMonths(m_AddMonthsIndex).ToString("yyyyMM"), typeof(decimal));
                        myColumnName.Add(DateTime.Parse(myStartTime).AddMonths(m_AddMonthsIndex).ToString("yyyy年MM月"));
                        m_AddMonthsIndex = m_AddMonthsIndex + 1;
                    }

                    return m_TagTrendData;
                }
                else if (m_IntervalTime < 100000000 && m_IntervalTime >= 1000000)   //1月以上1年以下按照日进行查找
                {
                    DataTable m_TagTrendData = new DataTable(DaysTable);

                    int m_AddDaysIndex = 0;
                    while (m_StartTime.AddDays(m_AddDaysIndex) <= m_EndTime)
                    {
                        m_TagTrendData.Columns.Add(DateTime.Parse(myStartTime).AddDays(m_AddDaysIndex).ToString("MMdd"), typeof(decimal));
                        myColumnName.Add(DateTime.Parse(myStartTime).AddDays(m_AddDaysIndex).ToString("MM月dd日"));
                        m_AddDaysIndex = m_AddDaysIndex + 1;
                    }

                    return m_TagTrendData;
                }
                else if (m_IntervalTime < 1000000 && m_IntervalTime >= 10000)   //1天以上1月以下按照时进行查找
                {
                    DataTable m_TagTrendData = new DataTable(HoursTable);

                    int m_AddHoursIndex = 0;
                    while (m_StartTime.AddHours(m_AddHoursIndex) <= m_EndTime)
                    {
                        m_TagTrendData.Columns.Add(DateTime.Parse(myStartTime).AddHours(m_AddHoursIndex).ToString("ddHH"), typeof(decimal));
                        myColumnName.Add(DateTime.Parse(myStartTime).AddHours(m_AddHoursIndex).ToString("dd日HH时"));
                        m_AddHoursIndex = m_AddHoursIndex + 1;
                    }

                    return m_TagTrendData;
                }
                else if (m_IntervalTime < 10000)   //1天以下按照1分钟进行查找
                {
                    DataTable m_TagTrendData = new DataTable(MinutesTable);

                    int m_AddMinutesIndex = 0;
                    while (m_StartTime.AddMinutes(m_AddMinutesIndex) <= m_EndTime)
                    {
                        string m_MinutesTemp = DateTime.Parse(myStartTime).AddMinutes(m_AddMinutesIndex).ToString("HHmm");
                        //m_TagTrendData.Columns.Add(m_MinutesTemp.Substring(0,3) +　"0", typeof(decimal));
                        m_TagTrendData.Columns.Add(m_MinutesTemp, typeof(decimal));

                        string m_MinutesTemp1 = DateTime.Parse(myStartTime).AddMinutes(m_AddMinutesIndex).ToString("HH时mm分");
                        myColumnName.Add(m_MinutesTemp1);

                        m_AddMinutesIndex = m_AddMinutesIndex + 1;
                    }

                    return m_TagTrendData;
                }
                else                                            //真实时间查找
                {
                    return null;
                }
            }
            catch
            {
                return null;
            }
        }
        private static DataTable GetTagValues(string myTagTrendDataTableName, string myTagDataBase, string myTagItemId, string myStartTime, string myEndTime)
        {
            DataTable m_TagItemInfoTable = GetTagInfo(myTagDataBase, myTagItemId);
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

                if (myTagTrendDataTableName == YearsTable)          //年趋势
                {
                    DataTable m_TagValues = dal_HistoryTrend.GetYearTrendData(m_DataBaseName, m_DataTableName, m_DataFieldName, myStartTime, myEndTime, m_IsCumulant);
                    return m_TagValues;
                }
                else if (myTagTrendDataTableName == MonthsTable)          //月趋势
                {
                    DataTable m_TagValues = dal_HistoryTrend.GetMonthTrendData(m_DataBaseName, m_DataTableName, m_DataFieldName, myStartTime, myEndTime, m_IsCumulant);
                    return m_TagValues;
                }
                else if (myTagTrendDataTableName == DaysTable)          //日趋势
                {
                    DataTable m_TagValues = dal_HistoryTrend.GetDayTrendData(m_DataBaseName, m_DataTableName, m_DataFieldName, myStartTime, myEndTime, m_IsCumulant);
                    return m_TagValues;
                }
                else if (myTagTrendDataTableName == HoursTable)          //时趋势
                {
                    DataTable m_TagValues = dal_HistoryTrend.GetHourTrendData(m_DataBaseName, m_DataTableName, m_DataFieldName, myStartTime, myEndTime, m_IsCumulant);
                    return m_TagValues;
                }
                else if (myTagTrendDataTableName == MinutesTable)          //分趋势
                {
                    DataTable m_TagValues = dal_HistoryTrend.GetMinuteTrendData(m_DataBaseName, m_DataTableName, m_DataFieldName, myStartTime, myEndTime, m_IsCumulant);
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
        private static DataTable GetTagInfo(string myTagDataBase, string myTagItemId)
        {
            return dal_HistoryTrend.GetTagInfoByVariableName(myTagDataBase, ContrastTableName, myTagItemId);
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
