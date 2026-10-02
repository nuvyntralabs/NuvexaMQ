using System.Diagnostics;
using Nuventra.NuvexaMQ;

var host = Arg("host", "127.0.0.1");
var port = int.Parse(Arg("port", "5761"));
var publishers = int.Parse(Arg("publishers", "8"));
var each = int.Parse(Arg("each", "10000"));
var size = int.Parse(Arg("size", "128"));
var consumers = int.Parse(Arg("consumers", "8"));
var payload = new byte[size];
var subject = "bench.created";

await using (var setup = await NuvexaClient.ConnectAsync(host, port))
{
    await setup.EnsureStreamAsync(new StreamSpec("bench", ["bench.>"]) { PartitionCount = 1 });
    for (var i = 0; i < 200; i++)
        await setup.PublishAsync(subject, payload);
}

var publish = await Time(async () =>
{
    var tasks = new Task[publishers];
    for (var p = 0; p < publishers; p++)
    {
        tasks[p] = Task.Run(async () =>
        {
            await using var mq = await NuvexaClient.ConnectAsync(host, port);
            for (var i = 0; i < each; i++)
                await mq.PublishAsync(subject, payload);
        });
    }

    await Task.WhenAll(tasks);
});
var published = publishers * each;
Console.WriteLine($"publish connections={publishers} size={size} messages={published} {Rate(published, size, publish)}");

var consume = await Time(async () =>
{
    var tasks = new Task[consumers];
    for (var c = 0; c < consumers; c++)
    {
        tasks[c] = Task.Run(async () =>
        {
            await using var mq = await NuvexaClient.ConnectAsync(host, port);
            var spec = new ConsumeSpec("bench", "drain");
            while (true)
            {
                var batch = await mq.FetchAsync(spec, 256, TimeSpan.Zero);
                if (batch.Count == 0)
                    return;
                var acks = new Task[batch.Count];
                for (var i = 0; i < batch.Count; i++)
                    acks[i] = batch[i].AckAsync();
                await Task.WhenAll(acks);
            }
        });
    }

    await Task.WhenAll(tasks);
});
var drained = published + 200;
Console.WriteLine($"consume connections={consumers} size={size} messages={drained} {Rate(drained, size, consume)}");

static async Task<TimeSpan> Time(Func<Task> work)
{
    var watch = Stopwatch.StartNew();
    await work();
    watch.Stop();
    return watch.Elapsed;
}

static string Rate(int messages, int size, TimeSpan elapsed)
{
    var seconds = Math.Max(elapsed.TotalSeconds, 0.000001);
    var perSecond = messages / seconds;
    var megabytes = messages * (double)size / seconds / (1024 * 1024);
    return $"{perSecond:0} msg/s  {megabytes:0.0} MiB/s  {elapsed.TotalSeconds:0.00}s";
}

string Arg(string name, string fallback)
{
    var prefix = "--" + name + "=";
    var match = args.FirstOrDefault(arg => arg.StartsWith(prefix, StringComparison.Ordinal));
    return match is null ? fallback : match[prefix.Length..];
}
