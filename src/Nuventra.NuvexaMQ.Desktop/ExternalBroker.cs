using System.ComponentModel;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Avalonia.Threading;

namespace Nuventra.NuvexaMQ.Desktop;

internal static class ExternalBroker
{
    private const string MacService = "com.nuventra.nuvexamq";

    public static bool IsAddressInUse(Exception exception)
    {
        if (exception is AggregateException aggregate)
            return aggregate.InnerExceptions.Any(IsAddressInUse);

        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is SocketException socket && socket.SocketErrorCode == SocketError.AddressAlreadyInUse)
                return true;
            if (current.Message.Contains("address already in use", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    public static async Task<bool> MacServiceLoadedAsync(CancellationToken cancellationToken = default)
    {
        if (!OperatingSystem.IsMacOS())
            return false;
        var result = await RunAsync("/bin/launchctl", ["print", $"system/{MacService}"], cancellationToken).ConfigureAwait(false);
        return result.Code == 0;
    }

    public static Task<string?> BootoutMacServiceAsync(CancellationToken cancellationToken = default)
    {
        if (Dispatcher.UIThread.CheckAccess())
            return BootoutOnUiThreadAsync(cancellationToken);

        var done = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        Dispatcher.UIThread.Post(() =>
        {
            _ = CompleteBootoutAsync(done, cancellationToken);
        });
        return done.Task;
    }

    private static async Task CompleteBootoutAsync(TaskCompletionSource<string?> done, CancellationToken cancellationToken)
    {
        try
        {
            done.SetResult(await BootoutOnUiThreadAsync(cancellationToken).ConfigureAwait(true));
        }
        catch (Exception ex)
        {
            done.SetException(ex);
        }
    }

    private static async Task<string?> BootoutOnUiThreadAsync(CancellationToken cancellationToken)
    {
        var bootout = await RunAsync("/usr/bin/osascript",
        [
            "-e",
            $"do shell script \"launchctl bootout system/{MacService}\" with administrator privileges"
        ], cancellationToken).ConfigureAwait(true);
        return bootout.Code == 0 ? null : CleanOsascript(bootout.Error, bootout.Output);
    }

    public static async Task ReleaseAsync(IReadOnlyList<int> ports, Action<string> log, CancellationToken cancellationToken = default)
    {
        if (OperatingSystem.IsLinux())
            await StopLinuxServiceAsync(log, cancellationToken).ConfigureAwait(false);
        else if (OperatingSystem.IsWindows())
            await StopWindowsServiceAsync(log, cancellationToken).ConfigureAwait(false);

        var scan = await ListenersAsync(ports, cancellationToken).ConfigureAwait(false);
        foreach (var holder in scan.Brokers)
            TryKill(holder.Pid);

        foreach (var port in ports)
        {
            if (!await WaitUntilFreeAsync(port, cancellationToken).ConfigureAwait(false))
                throw new InvalidOperationException($"Port {port} is still in use.");
        }

        log("NuvexaMQ stopped.");
    }

    private static async Task<ListenerScan> ListenersAsync(IReadOnlyList<int> ports, CancellationToken cancellationToken)
    {
        var found = new Dictionary<int, BrokerProcess>();
        var sawAnyone = false;
        foreach (var port in ports)
        {
            foreach (var pid in await PidsAsync(port, cancellationToken).ConfigureAwait(false))
            {
                sawAnyone = true;
                if (pid == Environment.ProcessId || found.ContainsKey(pid))
                    continue;
                if (!IsBroker(pid, await CommandAsync(pid, cancellationToken).ConfigureAwait(false)))
                    continue;
                found.Add(pid, new BrokerProcess(pid));
            }
        }

        return new ListenerScan(found.Values.ToList(), sawAnyone);
    }

    private static async Task<List<int>> PidsAsync(int port, CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
            return await WindowsPidsAsync(port, cancellationToken).ConfigureAwait(false);
        if (OperatingSystem.IsMacOS())
            return await MacPidsAsync(port, cancellationToken).ConfigureAwait(false);

        var lsof = "lsof";
        var result = await RunAsync(lsof, ["-nP", $"-iTCP:{port}", "-sTCP:LISTEN", "-t"], cancellationToken).ConfigureAwait(false);
        var pids = new List<int>();
        foreach (var line in result.Output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (int.TryParse(line, out var pid))
                pids.Add(pid);
        }

        return pids;
    }

    private static async Task<List<int>> MacPidsAsync(int port, CancellationToken cancellationToken)
    {
        var result = await RunAsync("/usr/sbin/netstat", ["-anv", "-p", "tcp"], cancellationToken).ConfigureAwait(false);
        var pids = new List<int>();
        var suffix = "." + port;
        foreach (var line in result.Output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 11 || !parts[5].Equals("LISTEN", StringComparison.Ordinal) || !parts[3].EndsWith(suffix, StringComparison.Ordinal))
                continue;
            var token = parts[10];
            var colon = token.LastIndexOf(':');
            if (colon > 0 && int.TryParse(token[(colon + 1)..], out var pid))
                pids.Add(pid);
        }

        return pids;
    }

    private static async Task<List<int>> WindowsPidsAsync(int port, CancellationToken cancellationToken)
    {
        var result = await RunAsync(Path.Combine(Environment.SystemDirectory, "netstat.exe"), ["-ano", "-p", "tcp"], cancellationToken).ConfigureAwait(false);
        var suffix = $":{port}";
        var pids = new List<int>();
        foreach (var line in result.Output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (!line.Contains("LISTENING", StringComparison.OrdinalIgnoreCase))
                continue;
            var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 5 || !parts[1].EndsWith(suffix, StringComparison.Ordinal))
                continue;
            if (int.TryParse(parts[^1], out var pid))
                pids.Add(pid);
        }

        return pids;
    }

    private static async Task<string> CommandAsync(int pid, CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
        {
            try
            {
                using var process = Process.GetProcessById(pid);
                return process.ProcessName;
            }
            catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or Win32Exception)
            {
                return "";
            }
        }

        var ps = OperatingSystem.IsMacOS() ? "/bin/ps" : "ps";
        var result = await RunAsync(ps, ["-p", pid.ToString(), "-o", "command="], cancellationToken).ConfigureAwait(false);
        return result.Code == 0 ? result.Output.Trim() : "";
    }

    private static bool IsBroker(int pid, string command)
    {
        if (pid == Environment.ProcessId || command.Length == 0)
            return false;
        if (command.Contains("Nuventra.NuvexaMQ.Desktop", StringComparison.OrdinalIgnoreCase))
            return false;
        return command.Contains("nuvexamq", StringComparison.OrdinalIgnoreCase)
            || command.Contains("NuvexaMQ.Server", StringComparison.OrdinalIgnoreCase);
    }

    private static async Task StopWindowsServiceAsync(Action<string> log, CancellationToken cancellationToken)
    {
        var query = await RunAsync(Path.Combine(Environment.SystemDirectory, "sc.exe"), ["query", "NuvexaMQ"], cancellationToken).ConfigureAwait(false);
        if (query.Code != 0 || !query.Output.Contains("RUNNING", StringComparison.OrdinalIgnoreCase))
            return;
        log("Stopping the NuvexaMQ service.");
        var stop = await RunAsync(Path.Combine(Environment.SystemDirectory, "sc.exe"), ["stop", "NuvexaMQ"], cancellationToken).ConfigureAwait(false);
        if (stop.Code != 0 && !stop.Output.Contains("1062", StringComparison.Ordinal))
            throw new InvalidOperationException(FirstLine(stop.Error, stop.Output, "Could not stop the NuvexaMQ service."));
    }

    private static async Task StopLinuxServiceAsync(Action<string> log, CancellationToken cancellationToken)
    {
        var active = await RunAsync("systemctl", ["is-active", "--quiet", "nuvexamq"], cancellationToken).ConfigureAwait(false);
        if (active.Code != 0)
            return;
        log("Stopping the nuvexamq service.");
        var stop = await RunAsync("systemctl", ["stop", "nuvexamq"], cancellationToken).ConfigureAwait(false);
        if (stop.Code == 0)
            return;
        stop = await RunAsync("sudo", ["-n", "systemctl", "stop", "nuvexamq"], cancellationToken).ConfigureAwait(false);
        if (stop.Code != 0)
            throw new InvalidOperationException(FirstLine(stop.Error, stop.Output, "Could not stop the nuvexamq service."));
    }

    private static void TryKill(int pid)
    {
        if (pid == Environment.ProcessId)
            return;
        try
        {
            using var process = Process.GetProcessById(pid);
            if (!process.HasExited)
                process.Kill(entireProcessTree: false);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or Win32Exception or AggregateException)
        {
        }
    }

    private static async Task<bool> WaitUntilFreeAsync(int port, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < 50; attempt++)
        {
            if (!await AcceptsAsync(port, cancellationToken).ConfigureAwait(false))
                return true;
            await Task.Delay(100, cancellationToken).ConfigureAwait(false);
        }

        return false;
    }

    private static async Task<bool> AcceptsAsync(int port, CancellationToken cancellationToken)
    {
        using var probe = new TcpClient();
        try
        {
            var connect = probe.ConnectAsync(IPAddress.Loopback, port, cancellationToken).AsTask();
            var finished = await Task.WhenAny(connect, Task.Delay(300, cancellationToken)).ConfigureAwait(false);
            if (finished != connect)
                return false;
            await connect.ConfigureAwait(false);
            return probe.Connected;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    private static async Task<CommandResult> RunAsync(string fileName, IReadOnlyList<string> args, CancellationToken cancellationToken)
    {
        var start = new ProcessStartInfo
        {
            FileName = fileName,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };
        foreach (var arg in args)
            start.ArgumentList.Add(arg);

        using var process = Process.Start(start) ?? throw new InvalidOperationException($"Could not run {fileName}.");
        var output = await process.StandardOutput.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
        var error = await process.StandardError.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
        await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
        return new CommandResult(process.ExitCode, output, error);
    }

    private static string CleanOsascript(string error, string output)
    {
        var text = FirstLine(error, output, "The NuvexaMQ service is still running.");
        const string marker = "execution error: ";
        var index = text.IndexOf(marker, StringComparison.Ordinal);
        if (index >= 0)
            text = text[(index + marker.Length)..].Trim();
        if (text.Contains("User canceled", StringComparison.OrdinalIgnoreCase) || text.Contains("-128", StringComparison.Ordinal))
            return "Stop was canceled. The broker is still running.";
        if (text.Contains("invalid thread", StringComparison.OrdinalIgnoreCase))
            return "macOS could not show the password dialog. The broker is still running.";
        return text;
    }

    private static string FirstLine(string error, string output, string fallback)
    {
        var text = string.IsNullOrWhiteSpace(error) ? output : error;
        text = text.Trim();
        if (text.Length == 0)
            return fallback;
        var line = text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).FirstOrDefault();
        return string.IsNullOrWhiteSpace(line) ? fallback : line;
    }

    private readonly record struct BrokerProcess(int Pid);

    private readonly record struct ListenerScan(List<BrokerProcess> Brokers, bool SawAnyone);

    private readonly record struct CommandResult(int Code, string Output, string Error);
}
