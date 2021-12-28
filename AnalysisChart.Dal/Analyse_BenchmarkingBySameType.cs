using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using AnalysisChart.IDal;
using AnalysisChart.Model;
namespace AnalysisChart.Dal
{
    public class Analyse_BenchmarkingBySameType : IAnalyse_BenchmarkingBySameType
    {
        private static readonly WebStyleBaseForEnergy.DbDataAdapter m_DbDataAdapter = new WebStyleBaseForEnergy.DbDataAdapter("ConnNXJC");
        public DataTable GetBenchmarkingDataValue(string myStartTime, string myEndTime, List<string> myOrganizationIdList)
        {
            string m_OrganizationIds = "NULL";
            if (myOrganizationIdList != null)
            {
                for (int i = 0; i < myOrganizationIdList.Count; i++)
                {
                    if (i == 0)
                    {
                        m_OrganizationIds = "'" + myOrganizationIdList[i] + "'";
                    }
                    else
                    {
                        m_OrganizationIds = m_OrganizationIds + ",'" + myOrganizationIdList[i] + "'";
                    }
                }
            }
            try
            {
                string m_Sql = @"select 
                                D.OrganizationID as OrganizationID, 
                                D.VariableId as VariableId,
                                sum(D.Value) as Value 
                                from (
                                select B.OrganizationID as OrganizationID,
                                B.VariableId as VariableId, 
                                B.TotalPeakValleyFlatB as Value
                                from tz_Balance A, balance_energy B
                                where A.StaticsCycle = 'month'
                                and A.BalanceId = B.KeyId
                                and (B.ValueType = 'ElectricityQuantity' or B.ValueType = 'MaterialWeight')
                                and A.TimeStamp >= '{0}'
                                and A.TimeStamp <='{1}'
                                and B.OrganizationID in ({2})) D
                                group by D.OrganizationID, D.VariableId";
                m_Sql = string.Format(m_Sql, myStartTime, myEndTime, m_OrganizationIds);

                DataSet mDataSet_BenchmarkingDataValue = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "BenchmarkingDataValueTable");
                return mDataSet_BenchmarkingDataValue.Tables["BenchmarkingDataValueTable"];
            }
            catch
            {
                return null;
            }

        }
        public DataTable GetElectricityConsumptionFormula(List<string> myOrganizationIdList, List<string> myVariableIdList)
        {
            string m_OrganizationIds = "NULL";
            string m_VariableIds = "NULL";
            if (myOrganizationIdList != null && myVariableIdList != null)
            {
                for (int i = 0; i < myOrganizationIdList.Count; i++)
                {
                    if (i == 0)
                    {
                        m_OrganizationIds = "'" + myOrganizationIdList[i] + "'";
                    }
                    else
                    {
                        m_OrganizationIds = m_OrganizationIds + ",'" + myOrganizationIdList[i] + "'";
                    }
                }
                for (int i = 0; i < myVariableIdList.Count; i++)
                {
                    if (i == 0)
                    {
                        m_VariableIds = "'" + myVariableIdList[i] + "'";
                    }
                    else
                    {
                        m_VariableIds = m_VariableIds + ",'" + myVariableIdList[i] + "'";
                    }
                }
            }
            try
            {
                string m_Sql = @"select A.OrganizationID,B.Name,B.LevelCode,B.VariableID,B.LevelType,C.ValueFormula
                                    from tz_Formula A, formula_FormulaDetail B
                                    left join balance_Energy_Template C on B.VariableId + '_ElectricityConsumption' = C.VariableId
                                    where A.OrganizationID in ({0})
                                    and A.Type =2
                                    and A.Enable = 1
                                    and A.State = 0
                                    and A.KeyID = B.KeyID
                                    and (B.LevelType <> 'MainMachine'
                                    or (B.LevelType = 'MainMachine' and B.VariableId + '_ElectricityConsumption' in ({1})))
                                    order by B.LevelCode";
                m_Sql = string.Format(m_Sql, m_OrganizationIds, m_VariableIds);

                DataSet mDataSet_CaculateFormulaTable = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "CaculateFormulaTable");
                return mDataSet_CaculateFormulaTable.Tables["CaculateFormulaTable"];
            }
            catch
            {
                return null;
            }
        }
        public DataTable GetOrganizationsLevelCode(string myStatisticalRange, List<string> myOrganizationIds)
        {
            string m_MainFrameDataBase = WebStyleBaseForEnergy.DbDataAdapter.MainFrameDataBase;
            string m_Sql = @"select 
                                A.LevelCode as LevelCode,
                                A.OrganizationID as OrganizationId,
                                A.Name as Name,
                                '0.00' as Value  
                                from system_Organization A, system_Organization B
                                where A.LevelType = '{0}'
                                and A.LevelCode like B.LevelCode + '%'
                                and B.OrganizationID in {1}";
            //
            string m_OrganizationIds = "";

            if (myOrganizationIds != null)
            {
                for (int i = 0; i < myOrganizationIds.Count; i++)
                {
                    if (i == 0)
                    {
                        m_OrganizationIds = "'" + myOrganizationIds[i] + "'";
                    }
                    else
                    {
                        m_OrganizationIds = m_OrganizationIds + ",'" + myOrganizationIds[i] + "'";
                    }
                }
            }
            m_Sql = string.Format(m_Sql, myStatisticalRange, "(" + m_OrganizationIds + ")");
            try
            {
                DataSet m_DataSet_OrganizationLevelCodeTable = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "OrganizationLevelCodeTable");
                if (myStatisticalRange == "Group")
                {
                    if (m_DataSet_OrganizationLevelCodeTable.Tables["OrganizationLevelCodeTable"].Rows.Count == 0)   //当没有集团授权功能时，手动增加数据授权，但是这样做表示所有人能查询集团总的能耗信息
                    {
                        m_DataSet_OrganizationLevelCodeTable.Tables["OrganizationLevelCodeTable"].Rows.Add(new string[] { "O", "zc_nxjc", "集团", "0.00" });
                    }
                }
                return m_DataSet_OrganizationLevelCodeTable.Tables["OrganizationLevelCodeTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }
        public DataTable GetStaticsItems(string myOrganizationType, string myModel, string myEquipmentCommonId, string mySpecifications, bool myHiddenMainMachine, string myKeyName, List<string> myOrganizations)
        {
            string m_Sql = "";
            string m_OrganizationTypeE = "";
            string m_VariableId = "";
            if (myOrganizationType == "熟料")
            {
                m_OrganizationTypeE = "clinker";
                m_VariableId = "clinkerBurning";
            }
            else
            {
                m_OrganizationTypeE = "cementmill";
                m_VariableId = "cementGrind";
            }
            if (myModel == "ClinkerAndCementmill")
            {
                m_Sql = @"Select 
                        A.OrganizationID as OrganizationId, 
                        A.Name as Name,
					    A.LevelCode as LevelCode,
                        A.LevelType as Type, 
                        '' as VariableId,
                        'balance_Energy' as TagTableName, 
                        '' as TagDataBase,
                        '' as Value 
                        from system_Organization A
					    where A.Enabled = 1 
						and (A.LevelType in('Company','Factory')
                        or (A.LevelType = 'ProductionLine' and A.Type = '{0}'))
                        and {2}
                    union
                        Select 
                        A.OrganizationID as OrganizationId, 
                        (Case when C.LevelType = 'ProductionLine' then A.Name else '' end) + C.Name as Name,
					    A.LevelCode + substring(C.LevelCode,4 ,len(C.LevelCode)-3) as LeveCode,
                        C.LevelType as Type, 
                        C.VariableID as VariableId,
                        'balance_Energy' as TagTableName, 
                        'NXJC' as TagDataBase,
                        '' as Value 
                        from system_Organization A, tz_Formula B, formula_FormulaDetail C, formula_FormulaDetail D {4}
					    where A.Enabled = 1 
                        and A.Type = '{0}' 
                        and B.Type = 2
                        and B.ENABLE = 1
                        and B.State = 0
                        and A.OrganizationID = B.OrganizationID
                        and B.KeyID = C.KeyID
                        and A.Type = '{0}' 
                        and A.LevelType = 'ProductionLine'
                        and C.LevelType <> 'ProductionLine'
                        {1}
                        and {2}
                        and D.Name like '%{3}%'
                        and B.KeyID = D.KeyID
						and CHARINDEX(C.LevelCode, D.LevelCode) > 0
                        and D.LevelType <> 'ProductionLine'
                        {5}";

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

                string m_LevelType = "";
                if (myHiddenMainMachine == true)      //如果是电耗并且隐藏主要设备
                {
                    m_LevelType = " and C.LevelType in ('ProductionLine','Process') and D.LevelType in ('ProductionLine','Process')";
                }
                else
                {
                    m_LevelType = " and C.LevelType in ('ProductionLine','Process','MainMachine')";
                }
                string m_EquipmentCondition = "";
                string m_EquipmentDataBase = "";
                if (myEquipmentCommonId != "All" && myEquipmentCommonId != "")
                {
                    m_EquipmentDataBase = ", equipment_EquipmentDetail E";
                    m_EquipmentCondition = string.Format(@"and D.VariableId = E.VariableId
                        and B.OrganizationID = E.ProductionLineId
                        and E.EquipmentCommonId = '{0}'", myEquipmentCommonId);
                    if (mySpecifications != "All" && mySpecifications != "")
                    {
                        m_EquipmentCondition = m_EquipmentCondition + string.Format(" and Specifications = '{0}'", mySpecifications);
                    }
                }
                if (m_SqlCondition != "")
                {
                    m_Sql = string.Format(m_Sql, myOrganizationType, m_LevelType, "(" + m_SqlCondition + ")", myKeyName, m_EquipmentDataBase, m_EquipmentCondition);
                }
                else
                {
                    m_Sql = string.Format(m_Sql, myOrganizationType, m_LevelType, "A.OrganizationID <> A.OrganizationID", myKeyName, m_EquipmentDataBase, m_EquipmentCondition);
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
            else
            {
                m_Sql = @"Select 
                        A.OrganizationID as OrganizationId, 
                        A.Name as Name,
					    A.LevelCode as LevelCode,
                        '{2}' as VariableId, 
                        A.Type as Type,
                        '' as Value
                        from system_Organization A {3}
					    where A.Enabled = 1 
						and (A.Type in ('{0}') or A.Type = '' or A.Type is null or A.Type = '分厂' or A.Type = '分公司')
                        and {1}
                        {4}";
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
                string m_EquipmentCondition = "";
                string m_EquipmentDataBase = "";
                if (mySpecifications != "All" && mySpecifications != "")
                {
                    m_EquipmentDataBase = ", system_Organization B, tz_Formula C, equipment_EquipmentDetail D";
                    m_EquipmentCondition = string.Format(@"and CHARINDEX(A.LevelCode, B.LevelCode) > 0
					    and B.Enabled = 1 
						and B.LevelType = 'ProductionLine'
						and B.OrganizationID = C.OrganizationID
						and C.Type = 2
						and C.ENABLE = 1
						and C.State = 0
						and C.OrganizationID = D.ProductionLineId
						and D.VariableId = '{0}'
                        and D.Specifications = '{1}'", m_VariableId, mySpecifications);
                }
                if (m_SqlCondition != "")
                {
                    m_Sql = string.Format(m_Sql, myOrganizationType, "(" + m_SqlCondition + ")", m_OrganizationTypeE, m_EquipmentDataBase, m_EquipmentCondition);
                }
                else
                {
                    m_Sql = string.Format(m_Sql, myOrganizationType, "A.OrganizationID <> A.OrganizationID", m_OrganizationTypeE, m_EquipmentDataBase, m_EquipmentCondition);
                }
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
    }
}


//, tz_Formula B, formula_FormulaDetail C, equipment_EquipmentDetail D