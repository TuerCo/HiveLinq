using Remotion.Linq.Clauses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HiveLinq.HiveQueryGeneration
{
    public class QueryPartsAggregator
    {
        public QueryPartsAggregator()
        {
            FromParts = new List<string>();
            WhereParts = new List<string>();
            OrderByParts = new List<string>();
            GroupByParts = new List<string>();
        }

        public string SelectPart { get; set; }

        private List<string> FromParts { get; set; }

        private List<string> WhereParts { get; set; }

        private List<string> OrderByParts { get; set; }

        private List<string> GroupByParts { get; set; }

        public void AddFromPart(IQuerySource querySource)
        {
            FromParts.Add(string.Format("{0} {1}", GetEntityName(querySource), querySource.ItemName));
        }

        public void AddWherePart(string formatString, params object[] args)
        {
            WhereParts.Add(string.Format(formatString, args));
        }

        public void AddOrderByPart(IEnumerable<string> orderings)
        {
            OrderByParts.Insert(0, StringUtility.Join<string>(", ", orderings));
        }

        public void AddGroupByPart(IEnumerable<string> orderings)
        {
            GroupByParts.Insert(0, StringUtility.Join<string>(", ", orderings));
        }

        public string BuildHQLString()
        {
            var stringBuilder = new StringBuilder();

            if (string.IsNullOrEmpty(SelectPart) || FromParts.Count == 0)
                throw new InvalidOperationException("A query must have a select part and at least one from part.");

            if (!SelectPart.Contains(".") && !SelectPart.Contains("count"))
                SelectPart = SelectPart + ".*";

            stringBuilder.AppendFormat("select {0}", SelectPart);
            stringBuilder.AppendFormat(" from {0}", StringUtility.Join<string>(", ", FromParts));

            if (WhereParts.Count > 0)
                stringBuilder.AppendFormat(" where {0}", StringUtility.Join<string>(" and ", WhereParts));

            if (GroupByParts.Count > 0)
                stringBuilder.AppendFormat(" group by {0}", StringUtility.Join<string>(" , ", GroupByParts));

            if (OrderByParts.Count > 0)
                stringBuilder.AppendFormat(" order by {0}", StringUtility.Join<string>(", ", OrderByParts));

            return stringBuilder.ToString();
        }

        private string GetEntityName(IQuerySource querySource)
        {
            HiveTableAttribute hiveTable = querySource.ItemType.GetCustomAttributes(typeof(HiveTableAttribute), true)
                                                               .Cast<HiveTableAttribute>()
                                                               .FirstOrDefault();

            return hiveTable.TableName;
        }
    }
}