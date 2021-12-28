using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
namespace AnalysisChart.IDal
{
    public interface IAnalyse_KPICommon
    {
        /// <summary>
        /// 根据ID列表获得ID列表的所有父节点（一直到根节点）和孩子节点
        /// </summary>
        /// <param name="myOrganizations">ID列表</param>
        /// <returns>该列表ID的所有父节点和孩子节点</returns>
        DataTable GetAllParentIdAndChildrenIdByIds(List<string> myOrganizations);
        /// <summary>
        /// 根据组织机构ID获得组织机构层次码
        /// </summary>
        /// <param name="myOrganizationId">组织机构ID</param>
        /// <returns>组织机构层次码</returns>
        string GetLevelCodeByOrganizationId(string myOrganizationId);
        /// <summary>
        /// 获得对标标准项
        /// </summary>
        /// <param name="myStatisticalMethod">统计模式</param>
        /// <param name="myValueType">计算类型</param>
        /// <param name="myOrganizations">组织机构</param>
        /// <returns>对标标准项表</returns>
        DataTable GetStandardItems(string myStatisticalMethod, string myValueType, string myStandardType, List<string> myOrganizations);
        /// <summary>
        /// 统计综合电耗标准项
        /// </summary>
        /// <param name="myStatisticalMethod"></param>
        /// <param name="myValueType"></param>
        /// <returns></returns>
        DataTable GetComprehensiveStandardItems(string myStatisticalMethod, string myValueType);
        DataTable GetEquipmentCommonInfo(string myOrganizationLineType);
        DataTable GetSpecificationsInfo(string myEquipmentCommonId);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="myDataBaseName"></param>
        /// <param name="myDataTableName"></param>
        /// <param name="myFieldName"></param>
        /// <param name="myStartTime"></param>
        /// <param name="myEndTime"></param>
        /// <param name="myIsCumulant"></param>
        /// <returns></returns>
        DataTable GetDayDCSData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant);
        DataTable GetMonthDCSData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant);

        //用户自定义标签组
        int SaveCustomDefineTags(string myTagsGroupName, string myTagInfoJson, string myRemark, string myUserId, string myTagGroupType);
        DataTable GetCustomDefineTagsGroup(string myUserId, string myTagGroupType);
        DataTable GetTagGroupJsonById(string myTagGroupId);
        int DeleteAllCustomDefineTagsByUserId(string myUserId, string myTagGroupType);
        int DeleteCustomDefineTagsByTagsGroupId(string myTagGroupId, string myTagGroupType);
    }
}
