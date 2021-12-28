using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
namespace AnalysisChart.IDal
{
    public interface IAnalyse_ProductionHorizontalComparison
    {
        DataTable GetEquipmentInfo(List<string> myDataValidIdGroup);
        DataTable GetStaticsItems(string myValueType, string myEquipmentCommonId, string mySpecificationsId, List<string> myOrganizations);
    }
}
