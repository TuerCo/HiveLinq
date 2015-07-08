using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HiveLinq.HiveQueryGeneration
{
    public static class StringUtility
    {
        public static string Join<T>(string separator, IEnumerable<T> values)
        {
#if !NET_3_5
            return string.Join(separator, values);
#else
      if (typeof (T) == typeof (string))
        return string.Join (separator, values.Cast<string>().ToArray());
      else
        return string.Join (separator, values.Select (v=>v.ToString()).ToArray());
#endif
        }
    }
}
