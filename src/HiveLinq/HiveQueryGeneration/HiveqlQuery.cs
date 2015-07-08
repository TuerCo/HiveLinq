using Apache.Hadoop.Hive;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace HiveLinq.HiveQueryGeneration
{
    public class HiveqlQuery
    {
        private readonly ThriftHive.Iface _client;
        private string _statement;

        public HiveqlQuery(ThriftHive.Iface client, string statement)
        {
            _client = client;
            _statement = statement;
        }

        public string GetQuery()
        {
            string pattern = @"(<[a-z]*>*)";
            string replacement = "items";
            Regex rgx = new Regex(pattern);
            var querystring = rgx.Replace(_statement, replacement);

            return querystring;
        }

        public IEnumerable<T> Enumerable<T>()
        {
            var querystring = String.Empty;
            var resultType = typeof(T);
            var hasFilter = false;

            string pattern = @"(<[a-z]*>*)";
            string replacement = "items";
            Regex rgx = new Regex(pattern);
            querystring = rgx.Replace(_statement, replacement);

            Console.WriteLine(querystring);

            _client.execute(querystring);
            var result = _client.fetchAll();

            if (typeof(T) == typeof(string))
            {
                return result.Cast<T>();
            }

            if (typeof(T) == typeof(int))
            {
                return result.Select(x => Int32.Parse(x)).Cast<T>();
            }

            return ToList<T>(result);

            //if (_queryParts.ReturnCount)
            //{
            //    querystring += "/$count";
            //}

            //querystring += "?$format=json&";

            //if (_queryParts.Take.HasValue)
            //    querystring += "$top=" + _queryParts.Take.Value + "&";

            //if (_queryParts.Skip.HasValue)
            //    querystring += "$skip=" + _queryParts.Skip.Value + "&";

            //if (!String.IsNullOrEmpty(_queryParts.OrderBy))
            //    querystring += "$orderby=" + _queryParts.OrderBy + "&";

            //var filter = SeparatedStringBuilder.Build(" and ", _queryParts.WhereParts);
            //if (!String.IsNullOrEmpty(filter))
            //    querystring += "$filter=" + filter + "&";

            //if (!String.IsNullOrEmpty(_queryParts.SelectPart))
            //    querystring += "$select=" + _queryParts.SelectPart + "&";

            //var fullUrl = _url + "/" + _collectionName + querystring;
            //var json = UrlHelper.Get(fullUrl);

            //// Netflix retuns a separate array inside d when a filter is used for some reason, so hard-coded check for now during tests
            //hasFilter = !querystring.EndsWith("$format=json&");

            ////var json = ODataRequest.Execute(fullUrl, "POST", _queryParts.BuildODataApiPostData(), "application/json");

            //JObject res;
            //// check for Count() [Int32] and LongCOunt() [Int64]
            //if (_queryParts.ReturnCount && (resultType == typeof(Int32) || resultType == typeof(Int64)))
            //{
            //    var results = new List<T>();

            //    res = JObject.Parse(json);

            //    results.Add(res["total_rows"].ToObject<T>());
            //    return results;
            //}

            //// get the rows property and deserialize that
            //var jobject = JsonConvert.DeserializeObject(json) as JObject;
            //var rows = jobject["d"];
            //if (hasFilter)
            //{
            //    rows = rows["results"];
            //}

            //var items = rows.Select(row => row.ToObject<T>());

            //return items;
        }

        public static IList<T> ToList<T>(List<string> table)
        {
            IList<PropertyInfo> properties = typeof(T).GetProperties().ToList();
            IList<T> result = new List<T>();

            Func<string, IList<PropertyInfo>, T> ParseRow = CreateItemFromRow<T>;

            if (CheckIfAnonymousType(typeof(T)))
            {
                ParseRow = CreateAnonymousItemFromRow<T>;
            }

            foreach (var row in table)
            {
                var item = ParseRow(row, properties);//CreateItemFromRow<T>(row, properties);
                result.Add(item);
            }

            return result;
        }

        //http://stackoverflow.com/questions/478013/how-do-i-create-and-access-a-new-instance-of-an-anonymous-class-passed-as-a-para
        private static T CreateAnonymousItemFromRow<T>(string row, IList<PropertyInfo> properties)
        {
            var rowSplitted = row.Split('\t');
            object[] objArray = new object[properties.Count];
            for (int i = 0; i < properties.Count; i++)
            {
                var property = properties[i];
                objArray[i] = Convert.ChangeType(rowSplitted[i], property.PropertyType);
            }
            return (T)Activator.CreateInstance(typeof(T), objArray);
        }

        private static T CreateItemFromRow<T>(string row, IList<PropertyInfo> properties)
        {
            T item = Activator.CreateInstance<T>();
            var rowSplitted = row.Split('\t');
            object[] objArray = new object[properties.Count];
            for (int i = 0; i < properties.Count; i++)
            {
                var property = properties[i];
                property.SetValue(item, Convert.ChangeType(rowSplitted[i], property.PropertyType), null);
            }
            return item;
        }

        internal void SetParameter(string p1, object p2)
        {
            _statement = _statement.Replace(":" + p1, p2.ToString());
        }

        //http://stackoverflow.com/questions/2483023/how-to-test-if-a-type-is-anonymous
        private static bool CheckIfAnonymousType(System.Type type)
        {
            if (type == null)
                throw new ArgumentNullException("type");

            // HACK: The only way to detect anonymous types right now.
            return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                && type.IsGenericType && type.Name.Contains("AnonymousType")
                && (type.Name.StartsWith("<>") || type.Name.StartsWith("VB$"))
                && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }
    }
}