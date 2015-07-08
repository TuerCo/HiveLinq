using Apache.Hadoop.Hive;
using Remotion.Linq;
using Remotion.Linq.Parsing.Structure;
using System;
using System.Linq;
using System.Linq.Expressions;

namespace HiveLinq
{
    /// <summary>
    /// Provides the main entry point to a LINQ query.
    /// </summary>
    public class HiveQueryable<T> : QueryableBase<T>
    {
        private static IQueryExecutor CreateExecutor(ThriftHive.Iface session)
        {
            return new HiveQueryExecutor(session);
        }

        // This constructor is called by our users, create a new IQueryExecutor.
        //public HiveQueryable(ThriftHive.Iface session)
        //    : base(QueryParser.CreateDefault(), CreateExecutor(session))
        //{
        //}

        //// This constructor is called indirectly by LINQ's query methods, just pass to base.
        //public HiveQueryable(IQueryProvider provider, Expression expression)
        //    : base(provider, expression)
        //{
        //}

        public HiveQueryable(IQueryParser queryParser, IQueryExecutor executor, ThriftHive.Iface db)
            : base(new HiveQueryProvider(typeof(HiveQueryable<>), queryParser, executor, db))
        {
        }

        public HiveQueryable(IQueryProvider provider, Expression expression, ThriftHive.Iface db)
            : base(provider, expression)
        {
        }

        public string GetQueryData()
        {
            var hiveQueryProvider = Provider as HiveQueryProvider;

            if (hiveQueryProvider == null)
                throw new NotSupportedException("AqlQueryable should be use with HiveQueryProvider");

            return hiveQueryProvider.GetQueryData(this.Expression);
        }
    }
}