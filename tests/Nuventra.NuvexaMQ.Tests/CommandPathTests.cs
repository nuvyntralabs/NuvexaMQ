using Nuventra.NuvexaMQ.Server;
using Xunit;

namespace Nuventra.NuvexaMQ.Tests;

public sealed class CommandPathTests
{
    [Fact]
    public void RenderBlock_exports_the_directory_once()
    {
        var block = CommandPath.RenderBlock(["/opt/nuvexamq", "/opt/nuvexamq"]);
        var expected = "# >>> nuvexamq >>>\n"
            + "if ! printf '%s' \":$PATH:\" | grep -F -q -- ':/opt/nuvexamq:'; then\n"
            + "  export PATH='/opt/nuvexamq':\"$PATH\"\n"
            + "fi\n"
            + "# <<< nuvexamq <<<\n";
        Assert.Equal(expected, block.Replace("\r\n", "\n"));
    }

    [Fact]
    public void Upsert_replaces_an_existing_block()
    {
        var original = "echo hi\n" + CommandPath.RenderBlock(["/opt/old"]) + "echo bye\n";
        var updated = CommandPath.Upsert(original, CommandPath.RenderBlock(["/opt/new"]));
        Assert.Contains(":/opt/new:", updated, StringComparison.Ordinal);
        Assert.DoesNotContain("/opt/old", updated, StringComparison.Ordinal);
        Assert.StartsWith("echo hi\n", updated, StringComparison.Ordinal);
        Assert.EndsWith("echo bye\n", updated, StringComparison.Ordinal);
    }

    [Fact]
    public void Development_output_is_not_added_to_path()
    {
        using var home = new TempHome();
        var dev = Path.Combine(home.Path, "repo", "bin", "Debug", "net10.0");
        Directory.CreateDirectory(dev);
        File.WriteAllText(Path.Combine(dev, CommandPath.HostFileName), "");

        var result = CommandPath.Install(dev, home.Path, registry: false);

        Assert.False(result.Changed);
        Assert.False(result.Ready);
        Assert.False(File.Exists(Profile(home.Path)));
    }

    [Fact]
    public void Install_adds_the_server_and_desktop_directories()
    {
        using var home = new TempHome();
        var server = Path.Combine(home.Layout, "Contents", "Resources", "server");
        var desktop = Path.Combine(home.Layout, "Contents", "MacOS");
        Directory.CreateDirectory(server);
        Directory.CreateDirectory(desktop);
        File.WriteAllText(Path.Combine(server, CommandPath.HostFileName), "");
        File.WriteAllText(Path.Combine(desktop, CommandPath.DesktopFileName), "");

        var fromDesktop = CommandPath.ExecutableDirectories(desktop);
        Assert.Contains(Path.GetFullPath(desktop).TrimEnd(Path.DirectorySeparatorChar), fromDesktop);
        Assert.Contains(Path.GetFullPath(server).TrimEnd(Path.DirectorySeparatorChar), fromDesktop);

        var fromServer = CommandPath.ExecutableDirectories(server);
        Assert.Contains(Path.GetFullPath(server).TrimEnd(Path.DirectorySeparatorChar), fromServer);
        Assert.Contains(Path.GetFullPath(desktop).TrimEnd(Path.DirectorySeparatorChar), fromServer);

        var first = CommandPath.Install(desktop, home.Path, registry: false);
        Assert.True(first.Changed);
        Assert.True(first.Ready);
        var profile = File.ReadAllText(Profile(home.Path));
        Assert.Contains(Path.GetFullPath(server), profile, StringComparison.Ordinal);
        Assert.Contains(Path.GetFullPath(desktop), profile, StringComparison.Ordinal);

        var second = CommandPath.Install(server, home.Path, registry: false);
        Assert.False(second.Changed);
        Assert.True(second.Ready);
        Assert.Equal("nuvexamq is already on PATH.", second.Message);
    }

    private static string Profile(string home) =>
        Path.Combine(home, OperatingSystem.IsMacOS() ? ".zprofile" : ".profile");

    private sealed class TempHome : IDisposable
    {
        public TempHome()
        {
            Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "nuvexamq-path-" + Guid.NewGuid().ToString("n"));
            Layout = System.IO.Path.Combine(Path, "NuvexaMQ.app");
            Directory.CreateDirectory(Path);
        }

        public string Path { get; }

        public string Layout { get; }

        public void Dispose() => Directory.Delete(Path, recursive: true);
    }
}
