using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HiveLinq
{
    public static class QueryableExtensions
    {
        public static string GetQueryData<T>(this IQueryable<T> source)
        {
            return source.AsHiveQueryable<T>().GetQueryData();
        }

        public static HiveQueryable<T> AsHiveQueryable<T>(this IQueryable<T> source)
        {
            var queryable = (source) as HiveQueryable<T>;

            if (queryable == null)
                throw new InvalidCastException("Queryable source is not type of AqlQueryable");

            return queryable;
        }
    }
}