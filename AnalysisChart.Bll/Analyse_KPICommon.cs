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
    public class Analyse_KPICommon
    {
        private const string AnalyseCycYear = "year";
        private const string AnalyseCycMonth = "month";
        private const string AnalyseCycCustomDefine = "CustomDefine";

        private const string ComparableIndex = "ComparableIndex";
        private const string ComparableStandard = "ComparableStandard";
        private const string DcsData = "DcsData";
        private const string ProductionLineStatics = "ProductionLineStatics";
        private const string EntityBenchmarkingStatics = "EntityBenchmarkingStatics";              //工序指标
        private const string ComprehensiveStatics = "ComprehensiveStatics";                      //综合指标
        private const string CustomDefine = "CustomDefine";

        private const string ContrastTableName = "DCSContrast";
        private const string HistoryDCSIncreasement = "HistoryDCSIncreasement";

        private const string TimeOfLastCycName = "同期";

        private const string StaticsCycleDay = "day";
        private const string StaticsCycleMonth = "month";

        //基于接口实例化动态链接库
        private static readonly IAnalyse_KPICommon dal_IAnalyse_KPICommon = DalFactory.DalFactory.GetKPICommonInstance();
        private static readonly IHistoryTrend dal_HistoryTrend = DalFactory.DalFactory.GetHistoryTrendInstance();
        private static readonly AutoSetParameters.AutoGetEnergyConsumption_V1 AutoGetEnergyConsumption_V1 = new AutoSetParameters.AutoGetEnergyConsumption_V1(new SqlServerDataAdapter.SqlServerDataFactory(GetDbConnectionString("ConnNXJC")));
        public static List<string> GetAllParentIdAndChildrenIdByIds(List<string> myOrganizations)
        {
            DataTable m_OrganizationIdTable = dal_IAnalyse_KPICommon.GetAllParentIdAndChildrenIdByIds(myOrganizations);
            List<string> m_OrganizationIdList = new List<string>();
            if (m_OrganizationIdTable != null)
            {
                for (int i = 0; i < m_OrganizationIdTable.Rows.Count; i++)
                {
                    m_OrganizationIdList.Add(m_OrganizationIdTable.Rows[i]["OrganizationId"].ToString());
                }
            }
            return m_OrganizationIdList;
        }
        public static string GetStandardItems(string myStatisticalMethod, string myValueType, string myStandardType, List<string> myOrganizations)
        {
            DataTable m_StandardItems = dal_IAnalyse_KPICommon.GetStandardItems(myStatisticalMethod, myValueType, myStandardType, myOrganizations);
            return EasyUIJsonParser.DataGridJsonParser.DataTableToJson(m_StandardItems);
        }
        public static string GetComprehensiveStandardItems(string myStatisticalMethod, string myValueType)
        {
            DataTable m_StandardItems = dal_IAnalyse_KPICommon.GetComprehensiveStandardItems(myStatisticalMethod, myValueType);
            return EasyUIJsonParser.DataGridJsonParser.DataTableToJson(m_StandardItems);
        }

        /////////////////////////////增加规格型号//////////////////////////
        public static string GetEquipmentCommonInfo(string myOrganizationLineType)
        {
            DataTable m_EquipmentCommonInfoTable = dal_IAnalyse_KPICommon.GetEquipmentCommonInfo(myOrganizationLineType);
            return EasyUIJsonParser.DataGridJsonParser.DataTableToJson(m_EquipmentCommonInfoTable);
        }
        public static string GetSpecificationsInfo(string myEquipmentCommonId)
        {
            DataTable m_StandardItems = dal_IAnalyse_KPICommon.GetSpecificationsInfo(myEquipmentCommonId);
            return EasyUIJsonParser.DataGridJsonParser.DataTableToJson(m_StandardItems);
        }
        ////////////////////////////以下是自定义标签组的操作///////////////////////////////
        /// <summary>
        /// ////////////////////////////////
        /// </summary>
        /// <param name="myTagsGroupName"></param>
        /// <param name="myRemark"></param>
        /// <param name="myTagInfoJson"></param>
        /// <param name="myUserId"></param>
        /// <returns></returns>
        public static string SaveCustomDefineTags(string myTagsGroupName, string myRemark, string myTagInfoJson, string myUserId, string myGroupId)
        {
            int m_SaveResult = dal_IAnalyse_KPICommon.SaveCustomDefineTags(myTagsGroupName, myRemark, myTagInfoJson, myUserId, myGroupId);
            if (m_SaveResult > 0)
            {
                return "1";
            }
            else
            {
                return "0";
            }
        }
        public static string GetCustomDefineTagsGroup(string myUserId, string myGroupId)
        {
            DataTable m_CustomDefineTagsGroupTable = dal_IAnalyse_KPICommon.GetCustomDefineTagsGroup(myUserId, myGroupId);
            string m_CustomDefineTagsGroupJson = EasyUIJsonParser.DataGridJsonParser.DataTableToJson(m_CustomDefineTagsGroupTable);
            return m_CustomDefineTagsGroupJson;
        }
        public static string GetTagGroupJsonById(string myTagGroupId)
        {
            DataTable m_CustomDefineTagsGroupTable = dal_IAnalyse_KPICommon.GetTagGroupJsonById(myTagGroupId);
            if (m_CustomDefineTagsGroupTable != null && m_CustomDefineTagsGroupTable.Rows.Count > 0)
            {
                string m_CustomDefineTagsGroupJson = m_CustomDefineTagsGroupTable.Rows[0]["TagGroupJson"].ToString();
                return m_CustomDefineTagsGroupJson;
            }
            else
            {
                return "{\"rows\":[],\"total\":0}";
            }

        }
        public static string DeleteAllCustomDefineTagsByUserId(string myUserId, string myGroupId)
        {
            int m_DeleteResult = dal_IAnalyse_KPICommon.DeleteAllCustomDefineTagsByUserId(myUserId, myGroupId);
            if (m_DeleteResult > 0)
            {
                return "1";
            }
            else
            {
                return "0";
            }

        }
        public static string DeleteCustomDefineTagsByTagsGroupId(string myTagGroupId, string myGroupId)
        {
            int m_DeleteResult = dal_IAnalyse_KPICommon.DeleteCustomDefineTagsByTagsGroupId(myTagGroupId, myGroupId);
            if (m_DeleteResult > 0)
            {
                return "1";
            }
            else
            {
                return "0";
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

        public static string GetChartDataJson(string myAnalyseCyc, string myStartTime, string myEndTime, string myChartType, string myTagInfoJson)
        {
            string m_StartTime = "";
            string m_EndTime = "";
            string m_LastCycStartTime = "";
            string m_LastCycEndTime = "";
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
                    m_EndTime = m_DateTime.AddYears(1).ToString("yyyy") + "-01-01";

                    m_LastCycStartTime = m_DateTime.AddYears(-1).ToString("yyyy") + "-01-01";
                    m_LastCycEndTime = m_StartTime;

                    m_StaticsCycleType = StaticsCycleMonth;
                }
                else if (myAnalyseCyc == AnalyseCycMonth)          //按日统计
                {
                    m_UnitX = "天";
                    DateTime m_DateTime = DateTime.Parse(myStartTime);
                    m_StartTime = m_DateTime.ToString("yyyy-MM") + "-01";
                    m_EndTime = m_DateTime.AddMonths(1).ToString("yyyy-MM") + "-01";

                    m_LastCycStartTime = m_DateTime.AddYears(-1).ToString("yyyy-MM") + "-01";
                    m_LastCycEndTime = m_DateTime.AddMonths(1).AddYears(-1).ToString("yyyy-MM") + "-01";

                    m_StaticsCycleType = StaticsCycleDay;
                }
                else if (myAnalyseCyc == AnalyseCycCustomDefine)   //用户自定义
                {
                    DateTime m_StartDateTime = DateTime.Parse(myStartTime);
                    DateTime m_EndDateTime = DateTime.Parse(myEndTime);
                    m_UnitX = "天";
                    m_StartTime = myStartTime;
                    m_EndTime = m_EndDateTime.AddDays(1).ToString("yyyy-MM-dd");


                    m_LastCycStartTime = m_StartDateTime.AddYears(-1).ToString("yyyy-MM-dd");
                    m_LastCycEndTime = m_EndDateTime.AddDays(1).AddYears(-1).ToString("yyyy-MM-dd");

                    m_StaticsCycleType = StaticsCycleDay;
                }

                List<string> m_ColumnNameList = new List<string>();
                //////创建报表结构
                m_ChartDataTableStruct = CreateChartDataTableStruct(myAnalyseCyc, m_StartTime, m_EndTime, m_ColumnNameList);     //生成存储数据的DataTable

                string[] m_RowsJson = EasyUIJsonParser.Infrastructure.JsonHelper.ArrayPicker("rows", myTagInfoJson);
                DataTable m_TagsInfoTable = EasyUIJsonParser.DataGridJsonParser.JsonToDataTable(m_RowsJson, GetTagsInfoTable());
                foreach (DataRow m_DataRow in m_TagsInfoTable.Rows)
                {
                    m_TagField = m_DataRow["TagId"].ToString();
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
                    else if (m_TagTabClass == EntityBenchmarkingStatics)               //工序指标
                    {
                        string m_VaribleId = m_DataRow["TagId"].ToString();
                        string m_OrganizationId = m_DataRow["TagItemId"].ToString();
                        ///////////////获得标签数据
                        DataTable m_TagValues = null;
                        if (TimeOfLastCycName == m_DataRow["SameTimeOfLastCyc"].ToString())        //如果是去年同期
                        {
                            m_TagValues = AnalysisKPI_EntityBenchmarking.GetEntityBenchmarkingStaticsDataTable(myAnalyseCyc, m_OrganizationId, m_VaribleId, m_LastCycStartTime, m_LastCycEndTime);
                        }
                        else
                        {
                            m_TagValues = AnalysisKPI_EntityBenchmarking.GetEntityBenchmarkingStaticsDataTable(myAnalyseCyc, m_OrganizationId, m_VaribleId, m_StartTime, m_EndTime);
                        }
                        if (m_TagValues != null)
                        {
                            string[] m_DataColumnsItem = GetDataColumnsItem(m_TagValues, m_ChartDataTableStruct);
                            m_ChartDataTableStruct.Rows.Add(m_DataColumnsItem);
                        }
                    }
                    else if (m_TagTabClass == ComprehensiveStatics)               //综合指标
                    {
                        string m_OrganizationId = m_DataRow["TagItemId"].ToString();
                     
                        string m_StatisticType = m_DataRow["TagStaticsType"].ToString() != "" ? (m_DataRow["TagStaticsType"].ToString()).Split('_')[1] : "";
                        string m_OrganizationType = m_DataRow["OtherInfo"].ToString();
                        string m_TagId = "";
                        if (m_OrganizationType == "熟料" && m_TagId == "")
                        {
                            m_TagId = "clinker";
                        }
                        else if (m_OrganizationType == "水泥磨" && m_TagId == "")
                        {
                            m_TagId = "cementmill";
                        }
                        string m_VariableId = m_TagId + "_" + m_DataRow["TagStaticsType"].ToString();
                        DataTable m_TagValues = null;
                        if (m_StaticsCycleType == StaticsCycleMonth)          //月统计
                        {
                            m_TagValues = Analyse_ComprehensiveBenchmarking.GetMonthComprehensiveStaticsData(m_OrganizationId, m_VariableId, m_StatisticType, m_StartTime, m_EndTime);
                        }
                        else if (m_StaticsCycleType == StaticsCycleDay)          //日统计
                        {
                            m_TagValues = Analyse_ComprehensiveBenchmarking.GetDayComprehensiveStaticsData(m_OrganizationId, m_VariableId, m_StatisticType, m_StartTime, m_EndTime);
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
                    while (m_StartDateTime < m_EndDateTime)
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
        private static string[] GetDataColumnsItem(DataTable myTagValues, DataTable myChartDataTableStruct)
        {
            int m_TagValuesIndex = 0;
            string[] m_DataColumnsItem = new string[myChartDataTableStruct.Columns.Count];    //创建一行
            for (int j = 0; j < myChartDataTableStruct.Columns.Count; j++)
            {
                m_DataColumnsItem[j] = "0";
                m_TagValuesIndex = 0;
                while (m_TagValuesIndex < myTagValues.Rows.Count)
                {
                    if (myChartDataTableStruct.Columns[j].ColumnName == myTagValues.Rows[m_TagValuesIndex]["VDate"].ToString())
                    {
                        m_DataColumnsItem[j] = myTagValues.Rows[m_TagValuesIndex]["TagValue"].ToString();
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

        public static string GetLevelCodeByOrganizationId(string myOrganizationId)
        {
            return dal_IAnalyse_KPICommon.GetLevelCodeByOrganizationId(myOrganizationId);
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

        public static decimal GetComprehensiveConsumptionData(string myVariableId, string myStartTime, string myEndTime, string myLevelCode, string myStaticsCycle)
        {
            if (myVariableId == "clinker_ElectricityConsumption_Comprehensive")    //熟料综合电耗
            {
                return GetClinkerPowerConsumption(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            else if (myVariableId == "clinker_CoalConsumption_Comprehensive")      //熟料综合煤耗
            {
                return GetClinkerCoalConsumption(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            else if (myVariableId == "clinker_EnergyConsumption_Comprehensive")      //熟料综合能耗
            {
                return GetClinkerEnergyConsumption(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }

            /////////////////////////////////////水泥磨/////////////////////////////////
            else if (myVariableId == "cementmill_ElectricityConsumption_Comprehensive")        //水泥综合电耗
            {
                return GetCementPowerConsumption(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            else if (myVariableId == "cementmill_CoalConsumption_Comprehensive")              //水泥综合煤耗
            {
                return GetCementCoalConsumption(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            else if (myVariableId == "cementmill_EnergyConsumption_Comprehensive")            //水泥综合能耗
            {
                return GetCementEnergyConsumption(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            else
            {
                return 0.0m;
            }
        }
        public static decimal GetComparableConsumptionData(string myVariableId, string myStartTime, string myEndTime, string myLevelCode, string myStaticsCycle)
        {
            if (myVariableId == "clinker_ElectricityConsumption_Comparable")      //熟料可比综合电耗
            {
                return GetClinkerPowerConsumptionComparable(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            else if (myVariableId == "clinker_CoalConsumption_Comparable")      //熟料可比综合煤耗
            {
                return GetClinkerCoalConsumptionComparable(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            else if (myVariableId == "clinker_EnergyConsumption_Comparable")      //熟料可比综合能耗
            {
                return GetClinkerEnergyConsumptionComparable(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            /////////////////////////////////////水泥磨/////////////////////////////////
            else if (myVariableId == "cementmill_ElectricityConsumption_Comparable")      //水泥可比综合电耗
            {
                return GetCementPowerConsumptionComparable(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            else if (myVariableId == "cementmill_CoalConsumption_Comparable")             //水泥可比综合煤耗
            {
                return GetCementCoalConsumptionComparable(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            else if (myVariableId == "cementmill_EnergyConsumption_Comparable")           //水泥可比综合能耗
            {
                return GetCementEnergyConsumptionComparable(myStaticsCycle, myStartTime, myEndTime, myLevelCode);
            }
            else
            {
                return 0.0m;
            }
        }
        ///////////////////////以下是计算综合能耗和可比综合能耗
        public static decimal GetClinkerPowerConsumption(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetClinkerPowerConsumptionWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetClinkerCoalConsumption(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetClinkerCoalConsumptionWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetClinkerEnergyConsumption(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetClinkerEnergyConsumptionWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetCementPowerConsumption(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetCementPowerConsumptionWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetCementCoalConsumption(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetCementCoalConsumptionWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetCementEnergyConsumption(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetCementEnergyConsumptionWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetClinkerPowerConsumptionComparable(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetClinkerPowerConsumptionComparableWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetClinkerCoalConsumptionComparable(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetClinkerCoalConsumptionComparableWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetClinkerEnergyConsumptionComparable(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetClinkerEnergyConsumptionComparableWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetCementPowerConsumptionComparable(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetCementPowerConsumptionComparableWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetCementCoalConsumptionComparable(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetCementCoalConsumptionComparableWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }
        public static decimal GetCementEnergyConsumptionComparable(string myStaticsCycle, string myStartTime, string myEndTime, string myLevelCode)
        {
            return AutoGetEnergyConsumption_V1.GetCementEnergyConsumptionComparableWithFormula(myStaticsCycle, myStartTime, myEndTime, myLevelCode).CaculateValue;
        }

        ////////////////导出/////////////
        public static void ExportExcelFile(string myFileType, string myFileName, string myData)
        {
            if (myFileType == "xls")
            {
                UpDownLoadFiles.DownloadFile.ExportExcelFile(myFileName, myData);
            }
        }

    }
}
