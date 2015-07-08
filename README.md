# HiveLinq

## What it's this?

It's a Linq provider for Hive! 
* It's a long a road and we only have some linq methods implemented....Read the tests to see the methods that are implemented.

## How to use it?

```csharp
int port = 10000;

var socket = new TSocket("", port);
var transport = new TBufferedTransport(socket);
var proto = new TBinaryProtocol(transport);
var client = new ThriftHive.Client(proto);

transport.Open();

var tt = HiveQueryFactory.Queryable<StarWarsTable>(client)
    .GroupBy(x => x.Age).Select(x => new { Age = x.Key }).ToList();

Console.WriteLine(tt.ToPrettyString());

//Table definition
HiveTable(TableName = "jjchiw_star_wars")]
public class StarWarsTable
{
    public string Name { get; set; }

    public string Planet { get; set; }

    public string Profession { get; set; }

    public int Age { get; set; }
}

//Simple extension methods that serializ an object to string
public static class Dumper
{
    public static string ToPrettyString(this object value)
    {
        return JsonConvert.SerializeObject(value, Formatting.Indented);
    }
}
```


## Nuget

No Nuget by now, it's in a very early stage