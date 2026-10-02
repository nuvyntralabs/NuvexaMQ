using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.WindowsServices;

namespace Nuventra.NuvexaMQ.Server;

static class BrokerService
{
    public static bool ShouldRunAsService() =>
        OperatingSystem.IsWindows() && WindowsServiceHelpers.IsWindowsService();

    public static async Task<int> RunAsync(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Services.AddWindowsService(options => options.ServiceName = "NuvexaMQ");
        builder.Services.AddSingleton(new ServiceStart(args));
        builder.Services.AddHostedService<BrokerWorker>();
        await builder.Build().RunAsync();
        return 0;
    }
}

sealed record ServiceStart(string[] Args);

sealed class BrokerWorker(ServiceStart start) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = Cli.LoadOptions(CliArgs.Parse(start.Args));
        await using var server = await NuvexaMqServer.StartAsync(options);
        StartupLog.WriteBanner(
            options,
            server.ListenPort,
            server.HealthPort,
            server.ManagementPort,
            server.ManagementHttpsPort,
            server.Tls,
            server.GeneratedHttpsCertificate);
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
        }
    }
}
