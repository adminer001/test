using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace AnalysisChart.IDal
{
    public interface IHistoryTrend
    {
        //void InitializeDbConn();
        /// <summary>
        /// 按分钟进行统计计算，以10分钟为一个单位
        /// </summary>
        /// <param name="myDataBaseName">数据库名称</param>
        /// <param name="myDataTableName">数据表名</param>
        /// <param name="myFieldName">数据字段名</param>
        /// <param name="myStartTime">开始时间</param>
        /// <param name="myEndTime">结束时间</param>
        /// <returns>数据表</returns>
        DataTable GetMinuteTrendData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant);
        /// <summary>
        /// 按小时进行统计计算，以1小时为一个单位
        /// </summary>
        /// <param name="myDataBaseName">数据库名称</param>
        /// <param name="myDataTableName">数据表名</param>
        /// <param name="myFieldName">数据字段名</param>
        /// <param name="myStartTime">开始时间</param>
        /// <param name="myEndTime">结束时间</param>
        /// <returns>数据表</returns>
        DataTable GetHourTrendData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant);
        /// <summary>
        /// 按天进行统计计算，以1天为一个单位
        /// </summary>
        /// <param name="myDataBaseName">数据库名称</param>
        /// <param name="myDataTableName">数据表名</param>
        /// <param name="myFieldName">数据字段名</param>
        /// <param name="myStartTime">开始时间</param>
        /// <param name="myEndTime">结束时间</param>
        /// <returns>数据表</returns>
        DataTable GetDayTrendData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant);
        /// <summary>
        /// 按月进行统计计算，以1月为一个单位
        /// </summary>
        /// <param name="myDataBaseName">数据库名称</param>
        /// <param name="myDataTableName">数据表名</param>
        /// <param name="myFieldName">数据字段名</param>
        /// <param name="myStartTime">开始时间</param>
        /// <param name="myEndTime">结束时间</param>
        /// <returns>数据表</returns>
        DataTable GetMonthTrendData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant);
        /// <summary>
        /// 按年进行统计计算，以1年为一个单位
        /// </summary>
        /// <param name="myDataBaseName">数据库名称</param>
        /// <param name="myDataTableName">数据表名</param>
        /// <param name="myFieldName">数据字段名</param>
        /// <param name="myStartTime">开始时间</param>
        /// <param name="myEndTime">结束时间</param>
        /// <returns>数据表</returns>
        DataTable GetYearTrendData(string myDataBaseName, string myDataTableName, string myFieldName, string myStartTime, string myEndTime, bool myIsCumulant);
        /// <summary>
        /// 根据变量名找到标签信息
        /// </summary>
        /// <param name="myVariableName">标签变量名</param>
        /// <returns>标签信息</returns>
        DataTable GetTagInfoByVariableName(string myDataBaseName, string myDataTableName, string myVariableName);
    }
}
