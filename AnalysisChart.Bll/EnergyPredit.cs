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
    public class EnergyPredit
    {
        //基于接口实例化动态链接库
        private static readonly IEnergyPredit dal_EnergyPredit = DalFactory.DalFactory.GetEnergyPreditInstance();
        private static readonly BasicData.Service.EnergyConsumptionPredict.EnergyPredict m_EnergyPredictLib = new BasicData.Service.EnergyConsumptionPredict.EnergyPredict();

        public static string GetEnergyPreditItems()
        {

            DataTable m_EnergyPreditItemsTable = dal_EnergyPredit.GetEnergyPreditItems();
            return EasyUIJsonParser.DataGridJsonParser.DataTableToJson(m_EnergyPreditItemsTable);
        }
        public static string GetEnergyPreditChart(string myOrgnizationId, string myDateTime, string myTagItemsJson)
        {
            string m_DateTime = myDateTime + "-01";
            DataTable m_EnergyPreditTable = m_EnergyPredictLib.Get_Forecast_ProductionLineEnergy(myOrgnizationId, m_DateTime);
            List<string> m_EnergyPreditRowName = new List<string>();
            string[] m_TagItemsId = myTagItemsJson.Split(',');          //需要的显示chart的标签

            DataTable m_EnergyPreditChartValueTable = GetChartDataTable(m_EnergyPreditTable, m_TagItemsId, ref m_EnergyPreditRowName);




            string m_UnitX = "水泥品种";
            string m_UnitY = "";
            string[] m_ColumnNameList = new string[] { "1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月" };

            string m_ChartData = EasyUIJsonParser.ChartJsonParser.GetGridChartJsonString(m_EnergyPreditChartValueTable, m_ColumnNameList, m_EnergyPreditRowName.ToArray(), m_UnitX, m_UnitY, 1);
            return m_ChartData;
        }
        private static DataTable GetChartDataTable(DataTable myEnergyPreditTable, string[] myTagItemsId, ref List<string> myEnergyPreditRowName)
        {
            DataTable m_ChartDataTable = new DataTable("ChartDataTable");
            m_ChartDataTable.Columns.Add("January", typeof(decimal));
            m_ChartDataTable.Columns.Add("February", typeof(decimal));
            m_ChartDataTable.Columns.Add("March", typeof(decimal));
            m_ChartDataTable.Columns.Add("April", typeof(decimal));
            m_ChartDataTable.Columns.Add("May", typeof(decimal));
            m_ChartDataTable.Columns.Add("June", typeof(decimal));
            m_ChartDataTable.Columns.Add("July", typeof(decimal));
            m_ChartDataTable.Columns.Add("August", typeof(decimal));
            m_ChartDataTable.Columns.Add("September", typeof(decimal));
            m_ChartDataTable.Columns.Add("October", typeof(decimal));
            m_ChartDataTable.Columns.Add("November", typeof(decimal));
            m_ChartDataTable.Columns.Add("December", typeof(decimal));
            if (myEnergyPreditTable != null)
            {
                int m_TagItemsIndex = -1;
                for (int i = 0; i < myTagItemsId.Length; i++)
                {
                    m_TagItemsIndex = FindTagItemById(myEnergyPreditTable, myTagItemsId[i]);
                    object[] m_ValueGroupTemp = new object[12];
                    if (m_TagItemsIndex >= 0)
                    {
                        for (int j = 0; j < 12; j++)
                        {
                            try
                            {
                                m_ValueGroupTemp[j] = (object)myEnergyPreditTable.Rows[m_TagItemsIndex][j + 6];
                            }
                            catch
                            {
                                m_ValueGroupTemp[j] = 0.0m;
                            }
                        }
                    }
                    else
                    {
                        for (int j = 0; j < 12; j++)
                        {
                            m_ValueGroupTemp[j] = 0.0m;
                        }
                    }
                    m_ChartDataTable.Rows.Add(m_ValueGroupTemp);
                    myEnergyPreditRowName.Add(myTagItemsId[i]);
                }
            }

            return m_ChartDataTable;
        }
        //从已经查找到的DataTable里面找到索引
        private static int FindTagItemById(DataTable myEnergyPreditTable, string myTagItemsId)
        {
            for (int i = 0; i < myEnergyPreditTable.Rows.Count; i++)
            {
                if (myEnergyPreditTable.Rows[i]["QuotasID"].ToString() == myTagItemsId)
                {
                    return i;
                }
            }
            return -1;
        }
    }

}
