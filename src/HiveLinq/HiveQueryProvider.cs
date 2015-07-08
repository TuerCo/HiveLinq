using Apache.Hadoop.Hive;
using HiveLinq.HiveQueryGeneration;
using Remotion.Linq;
using Remotion.Linq.Parsing.Structure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace HiveLinq
{
    public class HiveQueryProvider : QueryProviderBase
    {
        private ThriftHive.Iface _client;
        private System.Type _queryableType;

        public HiveQueryProvider(System.Type queryableType, IQueryParser queryParser, IQueryExecutor executor, ThriftHive.Iface client)
            : base(queryParser, executor)
        {
            _client = client;

            _queryableType = queryableType;
        }

        public string GetQueryData(Expression expression)
        {
            var queryModel = GenerateQueryModel(expression);

            var visitor = new HiveqlGeneratorQueryModelVisitor();
            visitor.VisitQueryModel(queryModel);

            var command = visitor.GetHiveqlCommand();
            var query = command.ExecuteQuery(_client);

            return query.GetQuery();
        }

        public override IQueryable<T> CreateQuery<T>(Expression expression)
        {
            return (IQueryable<T>)Activator.CreateInstance(_queryableType.MakeGenericType(typeof(T)), this, expression, _client);
        }
    }
}