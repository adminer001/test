using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisChart.IDal;
using AnalysisChart.Model;
using System.Data;

namespace AnalysisChart.Dal
{
    public class Analyse_QuantityDataWithQuery : IAnalyse_QuantityDataWithQuery
    {
        private static readonly WebStyleBaseForEnergy.DbDataAdapter m_DbDataAdapter = new WebStyleBaseForEnergy.DbDataAdapter("ConnNXJC");

        public DataTable GetBlankTable(string myDataBaseName, string myDataTableName)
        {
            string m_sql = string.Format(@"select top(0) * from {0}.dbo.{1}", myDataBaseName, myDataTableName);
            DataSet m_DataSet = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_sql, myDataTableName);
            if (m_DataSet != null)
            {
                DataTable m_DataTable = m_DataSet.Tables[myDataTableName];
                return m_DataTable;
            }
            else
            {
                return null;
            }

        }
        public DataTable GetTableRowsValue(string myDataBaseName, string myDataTableName, string myDateColumnName, string[] myColumnName, string myStartTime, string myEndTime, string myAnalyiseType, string myStatisticsType)
        {
            string DateFormate = "";
            string m_SelectedColumnString = "";
            string m_GroupString = "";
            if (myStatisticsType == "MillTimes")
            {
                DateFormate = string.Format("convert(varchar(19),{0},120)", myDateColumnName);
            }
            else if (myStatisticsType == "Day")
            {
                DateFormate = string.Format("convert(varchar(10),{0},120)", myDateColumnName);
                m_GroupString = string.Format("group by convert(varchar(10),{0},120)", myDateColumnName);
            }
            else if (myStatisticsType == "Month")
            {
                DateFormate = string.Format("convert(varchar(7),{0},120)", myDateColumnName);
                m_GroupString = string.Format("group by convert(varchar(7),{0},120)", myDateColumnName);
            }
            else if (myStatisticsType == "Year")
            {
                DateFormate = string.Format("convert(varchar(4),{0},120)", myDateColumnName);
                m_GroupString = string.Format("group by convert(varchar(4),{0},120)", myDateColumnName);
            }

            if (myStatisticsType == "MillTimes")
            {
                for (var i = 0; i < myColumnName.Length; i++)
                {
                    if (m_SelectedColumnString == "")
                    {
                        m_SelectedColumnString = myColumnName[i];
                    }
                    else
                    {
                        m_SelectedColumnString = m_SelectedColumnString + "," + myColumnName[i];
                    }
                }
            }
            else if (myAnalyiseType == "max")
            {
                for (var i = 0; i < myColumnName.Length; i++)
                {
                    if (m_SelectedColumnString == "")
                    {
                        m_SelectedColumnString = "max(" + myColumnName[i] + ") as " + myColumnName[i];
                    }
                    else
                    {
                        m_SelectedColumnString = m_SelectedColumnString + "," + "max(" + myColumnName[i] + ") as " + myColumnName[i];
                    }
                }
            }
            else if (myAnalyiseType == "min")
            {
                for (var i = 0; i < myColumnName.Length; i++)
                {
                    if (m_SelectedColumnString == "")
                    {
                        m_SelectedColumnString = "min(" + myColumnName[i] + ") as " + myColumnName[i];
                    }
                    else
                    {
                        m_SelectedColumnString = m_SelectedColumnString + "," + "min(" + myColumnName[i] + ") as " + myColumnName[i];
                    }
                }
            }
            else if (myAnalyiseType == "avg")
            {
                for (var i = 0; i < myColumnName.Length; i++)
                {
                    if (m_SelectedColumnString == "")
                    {
                        m_SelectedColumnString = "avg(" + myColumnName[i] + ") as " + myColumnName[i];
                    }
                    else
                    {
                        m_SelectedColumnString = m_SelectedColumnString + "," + "avg(" + myColumnName[i] + ") as " + myColumnName[i];
                    }
                }
            }

            string m_sql = string.Format(@"select {2} as vDate, {3} from {0}.dbo.{1} where {4} >= '{5}' and {4} <= '{6}' {7}", myDataBaseName, myDataTableName, DateFormate, m_SelectedColumnString, myDateColumnName, myStartTime, myEndTime, m_GroupString);
            DataSet m_DataSet = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_sql, myDataTableName);
            if (m_DataSet != null)
            {
                DataTable m_DataTable = m_DataSet.Tables[myDataTableName];
                return m_DataTable;
            }
            else
            {
                return null;
            }
        }
    }
}
