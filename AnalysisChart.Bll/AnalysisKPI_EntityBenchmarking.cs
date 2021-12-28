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
    public class AnalysisKPI_EntityBenchmarking
    {
        private const string AnalyseCycYear = "year";
        private const string AnalyseCycMonth = "month";
        private const string AnalyseCycCustomDefine = "CustomDefine";
        private static readonly IAnalysisKPI_EntityBenchmarking dal_IEntityBenchmarking = DalFactory.DalFactory.GetEntityBenchmarkingInstance();
        public static string GetStaticsItems(string myOrganizationType, string myValueType, string myEquipmentCommonId, string mySpecifications, bool myHiddenMainMachine, string myKeyName, List<string> myOrganizations)
        {
            DataTable m_StaticsItems = dal_IEntityBenchmarking.GetStaticsItems(myOrganizationType, myValueType, myEquipmentCommonId, mySpecifications, myHiddenMainMachine, myKeyName, myOrganizations);
            return EasyUIJsonParser.TreeJsonParser.DataTableToJsonByLevelCode(m_StaticsItems, "LevelCode", "Name", new string[] { "OrganizationId", "TagColumnName", "TagTableName", "TagDataBase", "Type" });
        }

        public static DataTable GetMonthStaticsData(string myOrganizationId, string myVariableId, string myStartTime, string myEndTime)
        {
            DataTable m_MonthStaticsData = dal_IEntityBenchmarking.GetMonthStaticsData(myOrganizationId, myVariableId, myStartTime, myEndTime);
            return m_MonthStaticsData;
        }
        public static DataTable GetDayStaticsData(string myOrganizationId, string myVariableId, string myStartTime, string myEndTime)
        {
            DataTable m_DayStaticsData = dal_IEntityBenchmarking.GetDayStaticsData(myOrganizationId, myVariableId, myStartTime, myEndTime);
            return m_DayStaticsData;
        }
        /// <summary>
        /// 获得产线统计数据
        /// </summary>
        /// <param name="myAnalyseCyc"></param>
        /// <param name="myStartTime"></param>
        /// <param name="myEndTime"></param>
        /// <param name="myTagField"></param>
        /// <param name="myTagTable"></param>
        /// <param name="myTagDataBase"></param>
        /// <returns></returns>
        public static DataTable GetEntityBenchmarkingStaticsDataTable(string myAnalyseCyc, string myOrganizationId, string myVariableId, string myStartTime, string myEndTime)
        {
            if (myAnalyseCyc == AnalyseCycYear)                //按月统计
            {
                DataTable m_TagValues = dal_IEntityBenchmarking.GetMonthStaticsData(myOrganizationId, myVariableId, myStartTime, myEndTime);
                return m_TagValues;
            }
            else if (myAnalyseCyc == AnalyseCycMonth || myAnalyseCyc == AnalyseCycCustomDefine)          //按日统计
            {
                DataTable m_TagValues = dal_IEntityBenchmarking.GetDayStaticsData(myOrganizationId, myVariableId, myStartTime, myEndTime);
                return m_TagValues;
            }
            else
            {
                return null;
            }
        }
    }
}
