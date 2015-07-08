using Apache.Hadoop.Hive;
using System.Collections.Generic;

namespace HiveLinq.HiveQueryGeneration
{
    public class CommandData
    {
        public CommandData(string statement, NamedParameter[] namedParameters)
        {
            Statement = statement;
            NamedParameters = namedParameters;
        }

        public string Statement { get; private set; }

        public NamedParameter[] NamedParameters { get; private set; }

        public HiveqlQuery ExecuteQuery(ThriftHive.Iface client)
        {
            var query = new HiveqlQuery(client, Statement);

            foreach (var parameter in NamedParameters)
            {
                query.SetParameter(parameter.Name, parameter.Value);
            }

            return query;
        }
    }
}