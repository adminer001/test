using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using AnalysisChart.IDal;
using AnalysisChart.Model;

namespace AnalysisChart.Dal
{
    public class EnergyPredit : IEnergyPredit
    {
        private static readonly WebStyleBaseForEnergy.DbDataAdapter m_DbDataAdapter = new WebStyleBaseForEnergy.DbDataAdapter("ConnNXJC");
        public DataTable GetEnergyPreditItems()
        {
            string m_Sql = @"Select 
                    A.QuotasID as id,
                    A.IndicatorName as text
                    from forecast_ProductionLineEnergyConsumptionTemplate A  
                    order by A.IdSort";
            try
            {
                DataSet mDataSet_PreditItemsTable = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "PreditItemsTable");
                return mDataSet_PreditItemsTable.Tables["PreditItemsTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}
