using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using AnalysisChart.IDal;
using AnalysisChart.Model;
namespace AnalysisChart.Dal
{
    public class Analyse_KPICommon : IAnalyse_KPICommon
    {
        private static readonly WebStyleBaseForEnergy.DbDataAdapter m_DbDataAdapter = new WebStyleBaseForEnergy.DbDataAdapter("ConnNXJC");
        public DataTable GetAllParentIdAndChildrenIdByIds(List<string> myOrganizations)
        {
            string m_MainFrameDataBase = WebStyleBaseForEnergy.DbDataAdapter.MainFrameDataBase;
            string m_Sql = @"Select 
                                distinct A.OrganizationID as OrganizationId, 
                                A.Name as Name,
					            A.LevelCode as LeveCode
                                from system_Organization A, system_Organization B
					            where A.Enabled = 1 
                                and (A.LevelCode like B.LevelCode + '%' or CHARINDEX(A.LevelCode, B.LevelCode) > 0)
							    and B.OrganizationID in ({0})";
            string m_SqlCondition = "''";
            if (myOrganizations != null)
            {
                for (int i = 0; i < myOrganizations.Count; i++)
                {
                    if (i == 0)
                    {
                        m_SqlCondition = string.Format("'{0}'", myOrganizations[i]);
                    }
                    else
                    {
                        m_SqlCondition = m_SqlCondition + string.Format(",'{0}'", myOrganizations[i]);
                    }
                }
            }
            m_Sql = string.Format(m_Sql, m_SqlCondition);
            try
            {
                DataSet mDataSet_OrganizationType = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "OrganizationIdTable");
                return mDataSet_OrganizationType.Tables["OrganizationIdTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }
        public string GetLevelCodeByOrganizationId(string myOrganizationId)
        {
            string m_Sql = @"Select A.LevelCode as LevelCode from system_Organization A where A.OrganizationID  = '{0}'";
            m_Sql = string.Format(m_Sql, myOrganizationId);
            try
            {
                DataSet mDataSet_LevelCode = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "LevelCodeTable");
                if (mDataSet_LevelCode.Tables["LevelCodeTable"] != null && mDataSet_LevelCode.Tables["LevelCodeTable"].Rows.Count > 0)
                {
                    return mDataSet_LevelCode.Tables["LevelCodeTable"].Rows[0]["LevelCode"].ToString();
                }
                else
                {
                    return "";
                }
            }
            catch (Exception)
            {
                return "";
            }
        }
        public DataTable GetStandardItems(string myStatisticalMethod, string myValueType, string myStandardType, List<string> myOrganizations)
        {
            string m_Sql = @"Select 
                                B.StandardItemId as StandardItemId, 
                                B.OrganizationID as OrganizationId, 
                                A.KeyId as KeyId, 
                                A.StandardId as StandardId,
                                A.StandardName as StandardName,
                                B.Name as Name, 
                                A.Version as Version,
                                B.VariableId as VariableId, 
                                B.Unit as Unit,
                                B.StandardValue as StandardValue, 
                                B.StandardLevel as StandardLevel 
                                from analyse_KPI_Standard A, analyse_KPI_Standard_Detail B 
                                where A.StatisticalMethod = '{0}'
                                and A.Enabled = 1
                                and A.KeyId = B.KeyId
                                and B.Enabled = 1 
                                and B.ValueType = '{1}'
                                and B.StandardType = '{2}'
                                and (B.OrganizationID is null 
                                or B.OrganizationID in ({3}))
                                order by A.DisplayIndex, B.StandardLevel";
            string m_SqlCondition = "";
            if (myOrganizations != null)
            {
                for (int i = 0; i < myOrganizations.Count; i++)
                {
                    if (i == 0)
                    {
                        m_SqlCondition = string.Format("'{0}'", myOrganizations[i]);
                    }
                    else
                    {
                        m_SqlCondition = m_SqlCondition + string.Format(",'{0}'", myOrganizations[i]);
                    }
                }
            }

            try
            {
                m_Sql = string.Format(m_Sql, myStatisticalMethod, myValueType, myStandardType, m_SqlCondition);
                DataSet mDataSet_StandardItems = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "StandardItemsTable");
                return mDataSet_StandardItems.Tables["StandardItemsTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }

        public DataTable GetComprehensiveStandardItems(string myStatisticalMethod, string myValueType)
        {
            string m_Sql = @"Select 
                                B.StandardItemId as StandardItemId, 
                                B.OrganizationID as OrganizationId, 
                                A.KeyId as KeyId, 
                                A.StandardId as StandardId,
                                A.StandardName as StandardName,
                                B.Name as Name, 
                                A.Version as Version,
                                B.VariableId as VariableId, 
                                B.Unit as Unit,
                                B.StandardValue as StandardValue, 
                                B.StandardLevel as StandardLevel 
                                from analyse_KPI_Standard A, analyse_KPI_Standard_Detail B 
                                where A.StatisticalMethod = '{0}'
                                and A.Enabled = 1
                                and A.KeyId = B.KeyId
                                and B.Enabled = 1 
                                and B.ValueType = '{1}'
                                order by A.DisplayIndex, B.StandardLevel";
            try
            {
                m_Sql = string.Format(m_Sql, myStatisticalMethod, myValueType);
                DataSet mDataSet_StandardItems = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "StandardItemsTable");
                return mDataSet_StandardItems.Tables["StandardItemsTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }
        public DataTable GetEquipmentCommonInfo(string myOrganizationLineType)
        {
            string m_Sql = @"SELECT distinct B.EquipmentCommonId as id, 
                                  B.Name as text
                                  FROM equipment_EquipmentDetail A,equipment_EquipmentCommonInfo B, system_Organization C
                                  where A.Enabled = 1
                                  and A.EquipmentCommonId = B.EquipmentCommonId
                                  and C.Type = '{0}'
                                  and A.ProductionLineId = C.OrganizationID
								  and C.LevelType = 'ProductionLine'";
            m_Sql = string.Format(m_Sql, myOrganizationLineType);
            try
            {
                DataSet mDataSet_EquipmentCommonInfo = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "EquipmentCommonInfo");
                return mDataSet_EquipmentCommonInfo.Tables["EquipmentCommonInfo"];
            }
            catch (Exception)
            {
                return null;
            }
        }
        public DataTable GetSpecificationsInfo(string myEquipmentCommonId)
        {
            string m_Sql = @"SELECT distinct A.Specifications as id, 
                                  A.Specifications as text
                                  FROM equipment_EquipmentDetail A,equipment_EquipmentCommonInfo B
                                  where A.Enabled = 1
                                  and A.Specifications is not null
                                  and A.Specifications <> ''
                                  and A.EquipmentCommonId = B.EquipmentCommonId
                                  and B.EquipmentCommonId = '{0}'";
            m_Sql = string.Format(m_Sql, myEquipmentCommonId);
            try
            {
                DataSet mDataSet_SpecificationsInfo = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "SpecificationsInfo");
                return mDataSet_SpecificationsInfo.Tables["SpecificationsInfo"];
            }
            catch (Exception)
            {
                return null;
            }
        }
        public DataTable GetDayDCSData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant)
        {
            string m_FieldStringTemplate = "";
            if (myIsCumulant == true)
            {
                m_FieldStringTemplate = ", sum(case when A.{0} is null then 0 else A.{0} end) as TagValue ";
            }
            else
            {
                m_FieldStringTemplate = ", avg(case when A.{0} is null then 0 else A.{0} end) as TagValue ";
            }
            //replace(substring(CONVERT(varchar, max(A.vDate), 120 ),1,10),'-','') as VDate 
            string m_Sql = @"Select               
                    replace(substring(CONVERT(varchar, max(A.vDate), 120 ),6,5),'-','') as VDate 
                    {2} 
                    from {0}.dbo.{1} A
					where A.vDate > '{3}'
					and A.vDate <= '{4}'
                    group by substring(CONVERT(varchar, A.vDate, 120 ),1,10) 
                    order by max(A.vDate)";

            if (myFieldName != "")
            {
                try
                {
                    m_FieldStringTemplate = string.Format(m_FieldStringTemplate, myFieldName);
                    m_Sql = string.Format(m_Sql, myDataBaseName, myDataTableName, m_FieldStringTemplate, myStartTime, myEndTime);
                    DataSet mDataSet_HistoryTrendData = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "HistoryTrendDataTable");
                    return mDataSet_HistoryTrendData.Tables["HistoryTrendDataTable"];
                }
                catch (Exception)
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }
        public DataTable GetMonthDCSData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant)
        {
            string m_FieldStringTemplate = "";
            if (myIsCumulant == true)
            {
                m_FieldStringTemplate = ", sum(case when A.{0} is null then 0 else A.{0} end) as TagValue ";
            }
            else
            {
                m_FieldStringTemplate = ", avg(case when A.{0} is null then 0 else A.{0} end) as TagValue ";
            }
            string m_Sql = @"Select               
                    replace(substring(CONVERT(varchar, max(A.vDate), 120 ),6,2),'-','') as VDate 
                    {2} 
                    from {0}.dbo.{1} A
					where A.vDate > '{3}'
					and A.vDate <= '{4}'
                    group by substring(CONVERT(varchar, A.vDate, 120 ),1,7) 
                    order by max(A.vDate)";

            if (myFieldName != "")
            {
                try
                {
                    m_FieldStringTemplate = string.Format(m_FieldStringTemplate, myFieldName);
                    m_Sql = string.Format(m_Sql, myDataBaseName, myDataTableName, m_FieldStringTemplate, myStartTime, myEndTime);
                    DataSet mDataSet_HistoryTrendData = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "HistoryTrendDataTable");
                    return mDataSet_HistoryTrendData.Tables["HistoryTrendDataTable"];
                }
                catch (Exception)
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

        





        //用户自定义标签组
        public int SaveCustomDefineTags(string myTagsGroupName, string myRemark, string myTagInfoJson, string myUserId, string myTagGroupType)
        {
            string m_Sql = @" Insert into analyse_KPI_TagsGroup 
                ( TagGroupName, OrganizationID, TagGroupType, TagGroupJson, Remarks, Creator, CreateTime) 
                values
                ('{0}','{1}','{2}','{3}','{4}','{5}',{6});";
            m_Sql = m_Sql.Replace("{0}", myTagsGroupName);
            m_Sql = m_Sql.Replace("{1}", "");
            m_Sql = m_Sql.Replace("{2}", myTagGroupType);
            m_Sql = m_Sql.Replace("{3}", myTagInfoJson);
            m_Sql = m_Sql.Replace("{4}", myRemark);
            m_Sql = m_Sql.Replace("{5}", myUserId);
            m_Sql = m_Sql.Replace("{6}", "GETDATE()");
            try
            {
                return m_DbDataAdapter.MySqlDbDataAdaper.ExecuteNonQuery(m_Sql);
            }
            catch (Exception)
            {
                return 0;
            }
        }
        public DataTable GetCustomDefineTagsGroup(string myUserId, string myTagGroupType)
        {
            string m_Sql = @"Select               
                    A.TagGroupId as TagGroupId, 
                    A.TagGroupName as TagGroupName, 
                    A.OrganizationID as OrganizationId, 
                    A.TagGroupType as TagGroupType,
                    A.Remarks as Remarks, 
                    A.CreateTime as CreateTime 
                    from analyse_KPI_TagsGroup A
					where A.Creator = '{0}' 
					and A.TagGroupType = '{1}'
                    order by A.CreateTime desc";

            try
            {
                m_Sql = string.Format(m_Sql, myUserId, myTagGroupType);
                DataSet mDataSet_CustomTagsGroupTable = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "CustomTagsGroupTable");
                return mDataSet_CustomTagsGroupTable.Tables["CustomTagsGroupTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }
        public DataTable GetTagGroupJsonById(string myTagGroupId)
        {
            string m_Sql = @"Select               
                    A.TagGroupJson as TagGroupJson
                    from analyse_KPI_TagsGroup A
					where A.TagGroupId = '{0}'";

            try
            {
                m_Sql = string.Format(m_Sql, myTagGroupId);
                DataSet mDataSet_CustomTagsGroupTable = m_DbDataAdapter.MySqlDbDataAdaper.Fill(null, m_Sql, "CustomTagsGroupTable");
                return mDataSet_CustomTagsGroupTable.Tables["CustomTagsGroupTable"];
            }
            catch (Exception)
            {
                return null;
            }
        }
        public int DeleteAllCustomDefineTagsByUserId(string myUserId, string myTagGroupType)
        {
            string m_Sql = @"Delete from analyse_KPI_TagsGroup where Creator = '{0}' and TagGroupType = '{1}'";
            m_Sql = string.Format(m_Sql, myUserId, myTagGroupType);
            try
            {
                int m_ExecuteFlag = m_DbDataAdapter.MySqlDbDataAdaper.ExecuteNonQuery(m_Sql);
                return m_ExecuteFlag >= 1 ? 1 : m_ExecuteFlag;
            }
            catch (Exception)
            {
                return -1;
            }
        }
        public int DeleteCustomDefineTagsByTagsGroupId(string myTagGroupId, string myTagGroupType)
        {
            string m_Sql = @"Delete from analyse_KPI_TagsGroup where TagGroupId = '{0}' and TagGroupType = '{1}'";
            m_Sql = string.Format(m_Sql, myTagGroupId, myTagGroupType);
            try
            {
                int m_ExecuteFlag = m_DbDataAdapter.MySqlDbDataAdaper.ExecuteNonQuery(m_Sql);
                return m_ExecuteFlag >= 1 ? 1 : m_ExecuteFlag;
            }
            catch (Exception)
            {
                return -1;
            }
        }

    }

}
