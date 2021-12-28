using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using AnalysisChart.Model;
using AnalysisChart.DalFactory;
using AnalysisChart.IDal;
using Standard_GB16780_2012;
namespace AnalysisChart.Bll
{
    public class Analyse_BenchmarkingBySameType
    {
        private const string StaticsCycleDay = "day";
        private const string StaticsCycleMonth = "month";

        private static readonly IAnalyse_BenchmarkingBySameType dal_IAnalysisKPI = DalFactory.DalFactory.GetBenchmarkingBySameType();
        //private readonly static 
        public static string GetStaticsItems(string myOrganizationType, string myModel, string myEquipmentCommonId, string mySpecifications, bool myHiddenMainMachine, string myKeyName, List<string> myOrganizations)
        {
            string m_JsonValue = "";
            DataTable m_Value = dal_IAnalysisKPI.GetStaticsItems(myOrganizationType, myModel, myEquipmentCommonId, mySpecifications, myHiddenMainMachine, myKeyName, myOrganizations);
            m_JsonValue = EasyUIJsonParser.TreeJsonParser.DataTableToJsonByLevelCode(m_Value, "LevelCode", "Name",new string[] {"OrganizationId","Name","LevelCode","Value","VariableId","Type"});
            return m_JsonValue;
        }
        public static string GetBenchmarkingDataValue(string myStartTime, string myEndTime, string myValueType, string myTagInfoJson)
        {
            string m_ValueJson = "";
            string[] m_RowsJson = EasyUIJsonParser.Infrastructure.JsonHelper.ArrayPicker("rows", myTagInfoJson);
            DataTable m_TagsInfoTable = EasyUIJsonParser.DataGridJsonParser.JsonToDataTable(m_RowsJson, GetTagsInfoTable());

            if (m_TagsInfoTable != null && m_TagsInfoTable.Rows.Count > 0)
            {
                List<string> m_OrganizationList = new List<string>(0);
                List<string> m_VariableList = new List<string>(0);
                foreach (DataRow item in m_TagsInfoTable.Rows)           //工序电耗或者主要设备电耗
                {
                    if (myValueType == "ElectricityConsumption_Entity")
                    {
                        item["StatisticType"] = "Entity";
                    }
                    else if (myValueType == "ElectricityConsumption_Comprehensive" || myValueType == "CoalConsumption_Comprehensive" || myValueType == "EnergyConsumption_Comprehensive")
                    {
                        item["StatisticType"] = "Comprehensive";
                    }
                    else if (myValueType == "ElectricityConsumption_Comparable" || myValueType == "CoalConsumption_Comparable" || myValueType == "EnergyConsumption_Comparable")
                    {
                        item["StatisticType"] = "Comparable";
                    }

                    if (item["OrganizationId"] != null && item["OrganizationId"].ToString() != "")
                    {
                        if (item["StatisticType"].ToString() == "Entity")     //工序电耗或者是设备电耗
                        {
                            string m_OrganizationIdItem = item["OrganizationId"].ToString();
                            string m_VariableIdItem = item["TagItemId"].ToString();
                            m_VariableIdItem = m_VariableIdItem.Substring(0, m_VariableIdItem.Length - 7);
                            if (!m_OrganizationList.Contains(m_OrganizationIdItem))
                            {
                                m_OrganizationList.Add(m_OrganizationIdItem);
                            }
                            if (!m_VariableList.Contains(m_VariableIdItem))
                            {
                                m_VariableList.Add(m_VariableIdItem);
                            }
                        }
                    }
                }

                DataTable ElectricityConsumptionFormulaTable = dal_IAnalysisKPI.GetElectricityConsumptionFormula(m_OrganizationList, m_VariableList);
                DataTable BencharkingDataTable = dal_IAnalysisKPI.GetBenchmarkingDataValue(myStartTime, myEndTime, m_OrganizationList);
                string preFormula = "";
                foreach (DataRow dr in ElectricityConsumptionFormulaTable.Rows)                       //构造主要设备的公式
                {
                    if (dr["ValueFormula"] is DBNull)
                    {
                        preFormula = DealWithFormula(preFormula, dr["VariableId"].ToString().Trim());
                        dr["ValueFormula"] = preFormula;
                    }
                    else
                    {
                        preFormula = dr["ValueFormula"].ToString().Trim();
                    }
                }
                string[] calColumns = new string[] { "Value"};
                DataTable m_Result = EnergyConsumption.EnergyConsumptionCalculate.CalculateByOrganizationId(BencharkingDataTable, ElectricityConsumptionFormulaTable, "ValueFormula", calColumns);

                foreach (DataRow item in m_TagsInfoTable.Rows)
                {
                    string m_LevelCode = item["LevelCode"].ToString();
                    if (item["OrganizationId"] != null && item["OrganizationId"].ToString() != "")
                    {
                        if (item["StatisticType"].ToString() == "Comprehensive")       //如果计算综合电耗或者煤耗
                        {
                            item["Value"] = string.Format("{0:F2}", Analyse_KPICommon.GetComprehensiveConsumptionData(item["TagItemId"].ToString(), myStartTime, myEndTime, m_LevelCode, StaticsCycleMonth));
                        }
                        else if (item["StatisticType"].ToString() == "Comparable")     //如果计算可比综合电耗或者煤耗
                        {
                            item["Value"] = string.Format("{0:F2}", Analyse_KPICommon.GetComparableConsumptionData(item["TagItemId"].ToString(), myStartTime, myEndTime, m_LevelCode, StaticsCycleMonth));               
                        }
                        else if (item["StatisticType"].ToString() == "Entity")     //工序电耗或者是设备电耗
                        {
                            if (m_Result != null)
                            {
                                string m_OrganizationIdItem = item["OrganizationId"].ToString();
                                string m_VariableIdItem = item["TagItemId"].ToString();
                                m_VariableIdItem = m_VariableIdItem.Substring(0, m_VariableIdItem.Length - 7);
                                DataRow[] m_DataRow = m_Result.Select(string.Format("OrganizationID = '{0}' and VariableId + '_ElectricityConsumption' = '{1}'", m_OrganizationIdItem, m_VariableIdItem));
                                if (m_DataRow.Length > 0)
                                {
                                    decimal m_Value = m_DataRow[0]["Value"] != null ? (decimal)m_DataRow[0]["Value"] : 0.0m;
                                    item["Value"] = string.Format("{0:F2}", m_Value);   //item["TagItemId"].ToString()
                                }
                            }
                        }
                    }
                }

                m_ValueJson = CreateChartString(m_TagsInfoTable, m_TagsInfoTable.Rows[0]["StatisticName"].ToString(), m_TagsInfoTable.Rows[0]["StatisticType"].ToString());
            }
            return m_ValueJson;
        }

        /// <summary>
        /// 处理公式
        /// </summary>
        /// <param name="preFormula"></param>
        /// <param name="variableId"></param>
        /// <returns></returns>
        private static string DealWithFormula(string preFormula, string variableId)
        {
            if (preFormula.Contains('_'))
            {
                int num = preFormula.IndexOf('_');
                string subStr = preFormula.Substring(1, num - 1);
                return preFormula.Replace(subStr, variableId);
            }
            else
                return preFormula;
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
                m_DataTable.Columns.Add(myValueDataTable.Rows[i]["TagItemId"].ToString() + myValueDataTable.Rows[i]["OrganizationId"].ToString(), typeof(decimal));
                myColumnNameList.Add(myValueDataTable.Rows[i]["Name"].ToString());
                string m_Value = myValueDataTable.Rows[i]["Value"].ToString();
                m_ValueGroup[i] = m_Value == "" ? "0.0" : m_Value;
            }
            m_DataTable.Rows.Add(m_ValueGroup);
            return m_DataTable;
        }
    }
}
