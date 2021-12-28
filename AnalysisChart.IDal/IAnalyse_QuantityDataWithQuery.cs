using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace AnalysisChart.IDal
{
    public interface IAnalyse_QuantityDataWithQuery
    {
        DataTable GetBlankTable(string myDataBaseName, string myDataTableName);
        DataTable GetTableRowsValue(string myDataBaseName, string myDataTableName, string myDateColumnName, string[] myColumnName, string myStartTime, string myEndTime, string myAnalyiseType, string myStatisticsType);
    }
}
