using Apache.Hadoop.Hive;
using Remotion.Linq.Parsing.Structure;

namespace HiveLinq
{
    public class HiveQueryFactory
    {
        public static HiveQueryable<T> Queryable<T>(ThriftHive.Iface session)
        {
            var queryParser = QueryParser.CreateDefault();
            var executer = new HiveQueryExecutor(session);

            return new HiveQueryable<T>(queryParser, executer, session);
        }
    }
}