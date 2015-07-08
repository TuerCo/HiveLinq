using Apache.Hadoop.Hive;
using HiveLinq.HiveQueryGeneration;
using Remotion.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace HiveLinq
{
    // Called by re-linq when a query is to be executed.
    public class HiveQueryExecutor : IQueryExecutor
    {
        private readonly ThriftHive.Iface _client;

        public HiveQueryExecutor(ThriftHive.Iface client)
        {
            _client = client;
        }

        // Executes a query with a scalar result, i.e. a query that ends with a result operator such as Count, Sum, or Average.
        public T ExecuteScalar<T>(QueryModel queryModel)
        {
            return ExecuteCollection<T>(queryModel).Single();
        }

        // Executes a query with a single result object, i.e. a query that ends with a result operator such as First, Last, Single, Min, or Max.
        public T ExecuteSingle<T>(QueryModel queryModel, bool returnDefaultWhenEmpty)
        {
            return returnDefaultWhenEmpty ? ExecuteCollection<T>(queryModel).SingleOrDefault() : ExecuteCollection<T>(queryModel).Single();
        }

        // Executes a query with a collection result.
        public IEnumerable<T> ExecuteCollection<T>(QueryModel queryModel)
        {
            var commandData = HiveqlGeneratorQueryModelVisitor.GenerateHiveqlQuery(queryModel);

            var query = commandData.ExecuteQuery(_client);

            return query.Enumerable<T>();
        }
    }
}