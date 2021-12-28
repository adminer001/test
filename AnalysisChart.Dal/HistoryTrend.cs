using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using AnalysisChart.IDal;
using AnalysisChart.Model;
namespace AnalysisChart.Dal
{
    public class HistoryTrend : IHistoryTrend
    {
        private static readonly WebStyleBaseForEnergy.DbDataAdapter m_DbDataAdapter = new WebStyleBaseForEnergy.DbDataAdapter("ConnNXJC");

        public DataTable GetMinuteTrendData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant)
        {
            string m_FieldStringTemplate = "";
            if (myIsCumulant == true)
            {
                m_FieldStringTemplate = ", sum(A.{0}) as TagValue ";
            }
            else
            {
                m_FieldStringTemplate = ", avg(A.{0}) as TagValue ";
            }
            string m_Sql = @"Select               
                    replace(replace(replace(substring(CONVERT(varchar, max(A.vDate), 120 ),12,5),'-',''),' ',''),':','') as VDate 
                    {2} 
                    from {0}.dbo.{1} A
					where A.vDate > '{3}'
					and A.vDate <= '{4}'
                    group by substring(CONVERT(varchar, A.vDate, 120 ),1,16) 
                    order by max(A.vDate)";

            if (myFieldName != "")
            {
                try
                {
                    m_FieldStringTemplate = string.Format(m_FieldStringTemplate, myFieldName);
                    m_Sql = string.Format(m_Sql, myDataBaseName, myDataTableName, m_FieldStringTemplate, myStartTime, myEndTime);
                    DataSet mDataSet_HistoryTrendData = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "HistoryTrendDataTable");
                    return mDataSet_HistoryTrendData.Tables["HistoryTrendDataTable"];
                }
                catch (Exception)
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }
        public DataTable GetHourTrendData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant)
        {
            string m_FieldStringTemplate = "";
            if (myIsCumulant == true)
            {
                m_FieldStringTemplate = ", sum(A.{0}) as TagValue ";
            }
            else
            {
                m_FieldStringTemplate = ", avg(A.{0}) as TagValue ";
            }
            string m_Sql = @"Select               
                    replace(replace(replace(substring(CONVERT(varchar, max(A.vDate), 120 ),9,5),'-',''),' ',''),':','') as VDate
                    {2} 
                    from {0}.dbo.{1} A
					where A.vDate > '{3}'
					and A.vDate <= '{4}'
                    group by substring(CONVERT(varchar, A.vDate, 120 ),1,13) 
                    order by max(A.vDate)";

            if (myFieldName != "")
            {
                try
                {
                    m_FieldStringTemplate = string.Format(m_FieldStringTemplate, myFieldName);
                    m_Sql = string.Format(m_Sql, myDataBaseName, myDataTableName, m_FieldStringTemplate, myStartTime, myEndTime);
                    DataSet mDataSet_HistoryTrendData = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "HistoryTrendDataTable");
                    return mDataSet_HistoryTrendData.Tables["HistoryTrendDataTable"];
                }
                catch (Exception)
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }
        public DataTable GetDayTrendData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant)
        {
            string m_FieldStringTemplate = "";
            if (myIsCumulant == true)
            {
                m_FieldStringTemplate = ", sum(A.{0}) as TagValue ";
            }
            else
            {
                m_FieldStringTemplate = ", avg(A.{0}) as TagValue ";
            }
            string m_Sql = @"Select               
                    replace(replace(replace(substring(CONVERT(varchar, max(A.vDate), 120 ),6,5),'-',''),' ',''),':','') as VDate 
                    {2} 
                    from {0}.dbo.{1} A
					where A.vDate > '{3}'
					and A.vDate <= '{4}'
                    group by substring(CONVERT(varchar, A.vDate, 120 ),1,10) 
                    order by max(A.vDate)";

            if (myFieldName != "")
            {
                try
                {
                    m_FieldStringTemplate = string.Format(m_FieldStringTemplate, myFieldName);
                    m_Sql = string.Format(m_Sql, myDataBaseName, myDataTableName, m_FieldStringTemplate, myStartTime, myEndTime);
                    DataSet mDataSet_HistoryTrendData = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "HistoryTrendDataTable");
                    return mDataSet_HistoryTrendData.Tables["HistoryTrendDataTable"];
                }
                catch (Exception)
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }
        public DataTable GetMonthTrendData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant)
        {
            string m_FieldStringTemplate = "";
            if (myIsCumulant == true)
            {
                m_FieldStringTemplate = ", sum(A.{0}) as TagValue ";
            }
            else
            {
                m_FieldStringTemplate = ", avg(A.{0}) as TagValue ";
            }
            string m_Sql = @"Select               
                    replace(replace(replace(substring(CONVERT(varchar, max(A.vDate), 120 ),1,7),'-',''),' ',''),':','') as VDate
                    {2} 
                    from {0}.dbo.{1} A
					where A.vDate > '{3}'
					and A.vDate <= '{4}'
                    group by substring(CONVERT(varchar, A.vDate, 120 ),1,7) 
                    order by max(A.vDate)";

            if (myFieldName != "")
            {
                try
                {
                    m_FieldStringTemplate = string.Format(m_FieldStringTemplate, myFieldName);
                    m_Sql = string.Format(m_Sql, myDataBaseName, myDataTableName, m_FieldStringTemplate, myStartTime, myEndTime);
                    DataSet mDataSet_HistoryTrendData = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "HistoryTrendDataTable");
                    return mDataSet_HistoryTrendData.Tables["HistoryTrendDataTable"];
                }
                catch (Exception)
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }
        public DataTable GetYearTrendData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant)
        {
            string m_FieldStringTemplate = "";
            if (myIsCumulant == true)
            {
                m_FieldStringTemplate = ", sum(A.{0}) as TagValue ";
            }
            else
            {
                m_FieldStringTemplate = ", avg(A.{0}) as TagValue ";
            }
            string m_Sql = @"Select               
                    replace(replace(replace(substring(CONVERT(varchar, max(A.vDate), 120 ),1,4),'-',''),' ',''),':','') as VDate
                    {2} 
                    from {0}.dbo.{1} A
					where A.vDate > '{3}'
					and A.vDate <= '{4}'
                    group by substring(CONVERT(varchar, A.vDate, 120 ),1,4) 
                    order by max(A.vDate)";

            if (myFieldName != "")
            {
                try
                {
                    m_FieldStringTemplate = string.Format(m_FieldStringTemplate, myFieldName);
                    m_Sql = string.Format(m_Sql, myDataBaseName, myDataTableName, m_FieldStringTemplate, myStartTime, myEndTime);
                    DataSet mDataSet_HistoryTrendData = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "HistoryTrendDataTable");
                    return mDataSet_HistoryTrendData.Tables["HistoryTrendDataTable"];
                }
                catch (Exception)
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }
        public DataTable GetTagInfoByVariableName(string myDataBaseName, string myDataTableName, string myVariableName)
        {
            string m_Sql = @"Select 
                    A.VariableName as VariableName,
                    A.VariableDescription as VariableDescription, 
                    A.TableName as TableName, 
                    A.FieldName as FieldName,
                    A.IsCumulant as IsCumulant, 
                    A.CumulantName as CumulantName, 
                    C.MeterDatabase as CumulantDataBase 
                    from {0}.dbo.{1} A, system_Organization B, system_Database C
					where A.VariableName = '{2}'
                    and A.OrganizationID = B.OrganizationID
                    and B.DatabaseID = C.DatabaseID";
            try
            {
                m_Sql = string.Format(m_Sql, myDataBaseName, myDataTableName, myVariableName);
                DataSet mDataSet_TagInfo = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "TagInfoTable");
                return mDataSet_TagInfo.Tables["TagInfoTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}
