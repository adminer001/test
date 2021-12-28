using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace AnalysisChart.IDal
{
    public interface IAnalyse_BenchmarkingBySameType
    {
        DataTable GetBenchmarkingDataValue(string myStartTime, string myEndTime, List<string> myOrganizationIdList);
        DataTable GetElectricityConsumptionFormula(List<string> myOrganizationIdList,List<string> myVariableIdList);
        DataTable GetOrganizationsLevelCode(string myStatisticalRange, List<string> myOrganizationIds);
        DataTable GetStaticsItems(string myOrganizationType, string myModel, string myEquipmentCommonId, string mySpecifications, bool myHiddenMainMachine, string myKeyName, List<string> myOrganizations);
    }
}
