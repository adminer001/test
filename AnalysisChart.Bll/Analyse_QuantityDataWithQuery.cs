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
    public class Analyse_QuantityDataWithQuery
    {
        private const string StaticsCycleDay = "day";
        private const string StaticsCycleMonth = "month";
        private static readonly IAnalyse_QuantityDataWithQuery dal_IAnalyse_QuantityDataWithQuery = DalFactory.DalFactory.GetQuantityDataWithQueryInstance();
        public static string GetTableColumnNames(string myDataBaseName, string myDataTableName)
        {
            string ValueString = "";
            var BlankTable = dal_IAnalyse_QuantityDataWithQuery.GetBlankTable(myDataBaseName, myDataTableName);
            if (BlankTable != null && BlankTable.Columns != null)
            {
                for (var i = 0; i < BlankTable.Columns.Count; i++)
                {
                    if (BlankTable.Columns[i].ColumnName != "vDate")
                    {
                        if (ValueString == "")
                        {
                            ValueString = "\"" + BlankTable.Columns[i].ColumnName + "\"";
                        }
                        else
                        {
                            ValueString = ValueString + ",\"" + BlankTable.Columns[i].ColumnName + "\"";
                        }
                    }
                }
            }

            return "[" + ValueString + "]";
        }
        public static string GetTableRowsValue(string myDataBaseName, string myDataTableName, string myDateColumnName, string[] myColumnName, string myStartTime, string myEndTime, string myAnalyiseType, string myStatisticsType)
        {
            var ValueTable = dal_IAnalyse_QuantityDataWithQuery.GetTableRowsValue(myDataBaseName, myDataTableName, myDateColumnName, myColumnName, myStartTime, myEndTime, myAnalyiseType, myStatisticsType);
            string ValueString = EasyUIJsonParser.DataGridJsonParser.DataTableToJson(ValueTable);
            return ValueString;
        }
    }
}
