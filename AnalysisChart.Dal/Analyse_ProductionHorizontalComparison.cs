using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using AnalysisChart.IDal;
using AnalysisChart.Model;
namespace AnalysisChart.Dal
{
    public class Analyse_ProductionHorizontalComparison : IAnalyse_ProductionHorizontalComparison
    {
        private static readonly WebStyleBaseForEnergy.DbDataAdapter m_DbDataAdapter = new WebStyleBaseForEnergy.DbDataAdapter("ConnNXJC");
        public DataTable GetEquipmentInfo(List<string> myOrganizations)
        {
            string m_Condition = "";
            string m_Sql = @"select distinct C.EquipmentCommonId as id, C.Name as text from system_MasterMachineDescription A, equipment_EquipmentDetail B, equipment_EquipmentCommonInfo C, system_Organization D, system_Organization E
                                where A.ID = B.EquipmentId
                                and B.EquipmentCommonId = C.EquipmentCommonId
                                and A.OrganizationID = D.OrganizationID
                                and D.LevelCode like E.LevelCode + '%'
                                and E.OrganizationID in ({0})";

            try
            {
                if (myOrganizations != null && myOrganizations.Count > 0)
                {
                    for (int i = 0; i < myOrganizations.Count; i++)
                    {
                        if(i==0)
                        {
                            m_Condition = "'" + myOrganizations[i] + "'";
                        }
                        else
                        {
                            m_Condition = m_Condition + ",'" + myOrganizations[i] + "'";
                        }           
                    }
                    m_Sql = string.Format(m_Sql, m_Condition);
                    DataSet mDataSet_EquipmentInfo = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "EquipmentInfoTable");
                    return mDataSet_EquipmentInfo.Tables["EquipmentInfoTable"];
                }
                else
                {
                    return null;
                }
            }
            catch (Exception)
            {
                return null;
            }
        }

        public DataTable GetStaticsItems(string myValueType, string myEquipmentCommonId, string mySpecificationsId, List<string> myOrganizations)
        {
            string m_Condition = "";
            string m_EquipmentCommonCondition = "";
            string m_Sql = @"Select M.* from
                                (select CONVERT(varchar(64), A.ID) as EquipmentId, 
                                    A.OrganizationID as OrganizationId, 
                                    A.OutputField, 
                                    B.EquipmentName as Name,
                                    C.LevelCode as LevelCode, 
                                    B.EquipmentCommonId as EquipmentCommonId, 
                                    'Equipment' as LevelType
                                from system_MasterMachineDescription A, equipment_EquipmentDetail B, system_Organization C, system_Organization D
                                    where A.ID = B.EquipmentId
                                    {1}
                                    and A.OrganizationID = C.OrganizationID
                                    and C.LevelCode like D.LevelCode + '%'
                                    and D.OrganizationID in ({0})
                                union all
                                select '' as EquipmentId,
                                    A.OrganizationID as OrganizationId,
                                    '' as OutputField,
                                    A.Name as Name,
                                    A.LevelCode,
                                    '' as EquipmentCommonId,
                                    A.LevelType as LevelType
                                from system_Organization A, system_Organization B
                                    where (A.LevelCode like B.LevelCode + '%' or CHARINDEX(A.LevelCode, B.LevelCode) > 0)
                                    and B.OrganizationID in ({0})
                                    and (A.LevelType = 'Company' or A.LevelType = 'Factory')) M
                                order by M.LevelCode, M.Name";
            try
            {
                if (myOrganizations != null && myOrganizations.Count > 0)
                {
                    for (int i = 0; i < myOrganizations.Count; i++)
                    {
                        if (i == 0)
                        {
                            m_Condition = "'" + myOrganizations[i] + "'";
                        }
                        else
                        {
                            m_Condition = m_Condition + ",'" + myOrganizations[i] + "'";
                        }
                    }
                    if (myEquipmentCommonId != "")
                    {
                        m_EquipmentCommonCondition = string.Format(" and B.EquipmentCommonId = '{0}' ", myEquipmentCommonId);
                    }
                    if (mySpecificationsId != "")
                    {
                        m_EquipmentCommonCondition = m_EquipmentCommonCondition + string.Format(" and B.Specifications = '{0}' ", mySpecificationsId);
                    }
                    m_Sql = string.Format(m_Sql, m_Condition, m_EquipmentCommonCondition);
                    DataSet mDataSet_EquipmentInfo = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "EquipmentInfoTable");
                    return mDataSet_EquipmentInfo.Tables["EquipmentInfoTable"];
                }
                else
                {
                    return null;
                }
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}
