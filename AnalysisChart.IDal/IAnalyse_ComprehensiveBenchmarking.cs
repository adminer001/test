using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
namespace AnalysisChart.IDal
{
    public interface IAnalyse_ComprehensiveBenchmarking
    {
        DataTable GetComprehensiveStaticsItems(string myOrganizationType, string myValueType, bool myHiddenMainMachine, List<string> myOrganizations);
        DataTable GetMonthComprehensiveStaticsData(string myOrganizationId, string myVariableId, string myStartTime, string myEndTime);
        DataTable GetMonthComprehensiveStaticsTotalData(string myOrganizationId, string myVariableId, string myStartTime, string myEndTime);
    }
}
