using Apache.Hadoop.Hive;
using HiveLinq;
using NSubstitute;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HiveLinq.Tests
{
    [TestFixture]
    public class QueriesTests
    {
        [Test]
        public void when_simple_query_get_all()
        {
            var client = Substitute.For<ThriftHive.Iface>();

            var table = HiveQueryFactory.Queryable<StarWarsTable>(client);

            var query = table.GetQueryData();

            Assert.AreEqual("select items_0.* from jjchiw_star_wars items_0", query);
        }

        [Test]
        public void when_simple_query_get_when_clause()
        {
            var client = Substitute.For<ThriftHive.Iface>();

            var table = HiveQueryFactory.Queryable<StarWarsTable>(client).Where(x => x.Age > 25);

            var query = table.GetQueryData();

            Assert.AreEqual("select x.* from jjchiw_star_wars x where (x.Age > 25)", query);
        }

        [Test]
        public void when_simple_query_get_when_clause_and_select_clause()
        {
            var client = Substitute.For<ThriftHive.Iface>();

            var table = HiveQueryFactory.Queryable<StarWarsTable>(client).Where(x => x.Age > 25).Select(x => x.Name);

            var query = table.GetQueryData();

            Assert.AreEqual("select x.Name from jjchiw_star_wars x where (x.Age > 25)", query);
        }

        [Test]
        public void when_simple_query_get_when_clause_and_select_clause_anonymous()
        {
            var client = Substitute.For<ThriftHive.Iface>();

            var table = HiveQueryFactory.Queryable<StarWarsTable>(client).Where(x => x.Age > 25).Select(x => new { Name = x.Name, Age = x.Age });

            var query = table.GetQueryData();

            Assert.AreEqual("select x.Name, x.Age from jjchiw_star_wars x where (x.Age > 25)", query);
        }

        [Test]
        public void when_simple_query_get_when_clause_and_count()
        {
            //Not implemented yet
            //var client = Substitute.For<ThriftHive.Iface>();

            //HiveQueryFactory.Queryable<StarWarsTable>(client).Where(x => x.Age > 25).Count();
            //HiveQueryFactory.Queryable<StarWarsTable>(client).Where(x => x.Age > 25).Select(x => x.Name).Count();
        }
    }

    [HiveTable(TableName = "jjchiw_star_wars")]
    public class StarWarsTable
    {
        public string Name { get; set; }

        public string Planet { get; set; }

        public string Profession { get; set; }

        public int Age { get; set; }
    }
}