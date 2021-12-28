using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisChart.IDal;
using AnalysisChart.Model;
using System.Data;
namespace AnalysisChart.Dal
{
    public class Analyse_ComprehensiveBenchmarking : IAnalyse_ComprehensiveBenchmarking
    {
        private const string clinker = "熟料";
        private const string cementmill = "水泥磨";
        private static readonly WebStyleBaseForEnergy.DbDataAdapter m_DbDataAdapter = new WebStyleBaseForEnergy.DbDataAdapter("ConnNXJC");
        public DataTable GetComprehensiveStaticsItems(string myOrganizationType, string myValueType, bool myHiddenMainMachine, List<string> myOrganizations)
        {
            string m_Sql = @"Select 
                        A.OrganizationID as OrganizationId, 
                        A.Name as Name,
					    A.LevelCode as LevelCode,
                        D.VariableId as TagColumnName,
                        'balance_Energy' as TagTableName, 
                        A.Type as Type, 
                        '' as TagDataBase 
                        from system_Organization A
                        left join
                            (Select 
                               B.OrganizationID as OrganizationID,
                               C.VariableId as VariableId 
                               from  tz_Formula B, formula_FormulaDetail C 
                               where B.Type = 2
                               and B.ENABLE = 1
                               and B.State = 0
                               and B.KeyID = C.KeyID
							   and C.LevelType = 'ProductionLine') D on A.OrganizationID = D.OrganizationID
					    where A.Enabled = 1 
						and (A.Type  = '{0}' or A.Type is null or A.Type = '' or A.Type = '分厂' or A.Type = '分公司')
                        and {1}
                    ";

            string m_SqlConditionTemp = @" (A.LevelCode like '{0}%' 
                                       or CHARINDEX(A.LevelCode, '{0}') > 0) ";
            string m_SqlCondition = "";
            if (myOrganizations != null)
            {
                for (int i = 0; i < myOrganizations.Count; i++)
                {
                    if (i == 0)
                    {
                        m_SqlCondition = string.Format(m_SqlConditionTemp, myOrganizations[i]);
                    }
                    else
                    {
                        m_SqlCondition = m_SqlCondition + string.Format("or " + m_SqlConditionTemp, myOrganizations[i]);
                    }
                }
            }
            if (m_SqlCondition != "")
            {
                m_Sql = string.Format(m_Sql, myOrganizationType, "(" + m_SqlCondition + ")");
            }
            else
            {
                m_Sql = string.Format(m_Sql, myOrganizationType, "A.OrganizationID <> A.OrganizationID");
            }
            try
            {
                DataSet mDataSet_ProductionLine = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "StaticsItemsTable");
                return mDataSet_ProductionLine.Tables["StaticsItemsTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }

    
        public DataTable GetMonthComprehensiveStaticsData(string myOrganizationId, string myVariableId, string myStartTime, string myEndTime)
        {
            string m_Sql = @"select M.VDate as Vdate, 
                    sum(M.TagValue) as TagValue from (
                        Select               
                        substring(A.TimeStamp, 6,2) as VDate, 
                        (case when B.TotalPeakValleyFlat is null then 0 else B.TotalPeakValleyFlat end) as TagValue
                        from tz_Balance A, balance_Energy B 
					    where A.TimeStamp > '{0}'
					    and A.TimeStamp <= '{1}'
                        and B.OrganizationID = '{2}' 
                        and A.BalanceId = B.KeyId 
                        and B.VariableId = '{3}') M
                    group by M.VDate
                    order by M.VDate";

            try
            {
                m_Sql = string.Format(m_Sql, myStartTime, myEndTime, myOrganizationId, myVariableId);
                DataSet mDataSet_HistoryTrendData = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "HistoryTrendDataTable");
                return mDataSet_HistoryTrendData.Tables["HistoryTrendDataTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }
        public DataTable GetMonthComprehensiveStaticsTotalData(string myOrganizationId, string myVariableId, string myStartTime, string myEndTime)
        {
            string m_Sql = @"select M.VDate as Vdate, 
                    sum(M.TagValue) as TagValue from (
                        Select               
                        substring(A.TimeStamp, 6,2) as VDate, 
                        (case when B.TotalPeakValleyFlat is null then 0 else B.TotalPeakValleyFlat end) as TagValue
                        from tz_Balance A, balance_Energy B 
					    where A.TimeStamp > '{0}'
					    and A.TimeStamp <= '{1}'
                        and B.OrganizationID = '{2}' 
                        and A.BalanceId = B.KeyId 
                        and B.VariableId = '{3}') M
                    group by M.VDate
                    order by M.VDate";

            try
            {
                m_Sql = string.Format(m_Sql, myStartTime, myEndTime, myOrganizationId, myVariableId);
                DataSet mDataSet_HistoryTrendData = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "HistoryTrendDataTable");
                return mDataSet_HistoryTrendData.Tables["HistoryTrendDataTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}
