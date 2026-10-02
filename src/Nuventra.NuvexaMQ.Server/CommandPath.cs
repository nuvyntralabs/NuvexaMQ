using System.Runtime.InteropServices;
using System.Text;

namespace Nuventra.NuvexaMQ.Server;

/// <summary>
/// Puts the installed <c>nuvexamq</c> server and the desktop executable on the user PATH.
/// </summary>
public static class CommandPath
{
    public const string BlockStart = "# >>> nuvexamq >>>";

    public const string BlockEnd = "# <<< nuvexamq <<<";

    public static string? Note { get; set; }

    public static string HostFileName => OperatingSystem.IsWindows() ? "nuvexamq.exe" : "nuvexamq";

    public static string DesktopFileName => OperatingSystem.IsWindows() ? "Nuventra.NuvexaMQ.Desktop.exe" : "Nuventra.NuvexaMQ.Desktop";

    public static bool IsDevelopmentOutput(string directory)
    {
        var full = Path.GetFullPath(directory).Replace('\\', '/');
        return full.Contains("/bin/Debug/", StringComparison.OrdinalIgnoreCase)
            || full.Contains("/bin/Release/", StringComparison.OrdinalIgnoreCase);
    }

    public static CommandPathResult Install(string baseDirectory) =>
        Install(baseDirectory, Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), OperatingSystem.IsWindows());

    public static CommandPathResult Install(string baseDirectory, string home, bool registry)
    {
        if (IsDevelopmentOutput(baseDirectory))
            return new CommandPathResult(false, false, "");

        var directories = ExecutableDirectories(baseDirectory);
        if (directories.Count == 0)
            return new CommandPathResult(false, false, "NuvexaMQ command was not found next to this program.");

        var changed = registry
            ? WriteRegistryPath(directories)
            : WriteProfiles(home, directories);
        RememberProcessPath(directories);
        var message = changed
            ? "Added NuvexaMQ to PATH. Open a new terminal and run nuvexamq."
            : "nuvexamq is already on PATH.";
        return new CommandPathResult(changed, true, message);
    }

    public static IReadOnlyList<string> ExecutableDirectories(string baseDirectory)
    {
        var found = new List<string>();
        foreach (var candidate in Candidates(baseDirectory))
            Consider(found, candidate);
        return found;
    }

    public static string RenderBlock(IReadOnlyList<string> directories)
    {
        var builder = new StringBuilder();
        builder.AppendLine(BlockStart);
        foreach (var directory in Distinct(directories))
        {
            var dir = TrimSlash(directory);
            builder.Append("if ! printf '%s' \":$PATH:\" | grep -F -q -- ").Append(ShellSingleQuote(":" + dir + ":")).AppendLine("; then");
            builder.Append("  export PATH=").Append(ShellSingleQuote(dir)).AppendLine(":\"$PATH\"");
            builder.AppendLine("fi");
        }

        builder.AppendLine(BlockEnd);
        return builder.ToString();
    }

    public static string Upsert(string? existing, string block)
    {
        var text = existing ?? "";
        var start = text.IndexOf(BlockStart, StringComparison.Ordinal);
        var end = text.IndexOf(BlockEnd, StringComparison.Ordinal);
        if (start >= 0 && end > start)
        {
            end += BlockEnd.Length;
            if (end < text.Length && text[end] == '\r')
                end++;
            if (end < text.Length && text[end] == '\n')
                end++;
            return string.Concat(text.AsSpan(0, start), block, text.AsSpan(end));
        }

        if (text.Length == 0)
            return block;
        if (!text.EndsWith('\n'))
            text += "\n";
        return text + "\n" + block;
    }

    public static bool ContainsDirectory(string? pathValue, string directory)
    {
        if (string.IsNullOrWhiteSpace(pathValue))
            return false;

        var separator = OperatingSystem.IsWindows() ? ';' : ':';
        var target = Normalize(directory);
        foreach (var entry in pathValue.Split(separator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (string.Equals(Normalize(entry), target, PathComparison))
                return true;
        }

        return false;
    }

    private static bool WriteProfiles(string home, IReadOnlyList<string> directories)
    {
        Directory.CreateDirectory(home);
        var block = RenderBlock(directories);
        var changed = false;
        foreach (var file in ProfileFiles(home))
        {
            var existing = File.Exists(file) ? File.ReadAllText(file) : "";
            var updated = Upsert(existing, block);
            if (updated == existing)
                continue;
            File.WriteAllText(file, updated);
            changed = true;
        }

        return changed;
    }

    private static bool WriteRegistryPath(IReadOnlyList<string> directories)
    {
        var current = Environment.GetEnvironmentVariable("Path", EnvironmentVariableTarget.User) ?? "";
        var updated = PrependMissing(current, directories);
        if (string.Equals(updated, current, StringComparison.OrdinalIgnoreCase))
            return false;

        Environment.SetEnvironmentVariable("Path", updated, EnvironmentVariableTarget.User);
        BroadcastEnvironment();
        return true;
    }

    private static void RememberProcessPath(IReadOnlyList<string> directories)
    {
        var name = OperatingSystem.IsWindows() ? "Path" : "PATH";
        var current = Environment.GetEnvironmentVariable(name);
        Environment.SetEnvironmentVariable(name, PrependMissing(current, directories));
    }

    private static string PrependMissing(string? pathValue, IReadOnlyList<string> directories)
    {
        var separator = OperatingSystem.IsWindows() ? ';' : ':';
        var value = pathValue ?? "";
        var list = Distinct(directories);
        for (var i = list.Count - 1; i >= 0; i--)
        {
            if (ContainsDirectory(value, list[i]))
                continue;
            value = value.Length == 0 ? list[i] : list[i] + separator + value;
        }

        return value;
    }

    private static IEnumerable<string> ProfileFiles(string home)
    {
        if (OperatingSystem.IsMacOS())
        {
            yield return Path.Combine(home, ".zprofile");
            yield return Path.Combine(home, ".zshrc");
            var bashProfile = Path.Combine(home, ".bash_profile");
            if (File.Exists(bashProfile))
                yield return bashProfile;
            yield break;
        }

        if (OperatingSystem.IsLinux())
        {
            yield return Path.Combine(home, ".profile");
            yield return Path.Combine(home, ".bashrc");
            yield break;
        }

        yield return Path.Combine(home, ".profile");
    }

    private static IEnumerable<string> Candidates(string baseDirectory)
    {
        var root = Path.GetFullPath(baseDirectory);
        yield return root;
        yield return Path.GetFullPath(Path.Combine(root, "server"));
        yield return Path.GetFullPath(Path.Combine(root, "..", "server"));
        yield return Path.GetFullPath(Path.Combine(root, "..", "desktop"));
        yield return Path.GetFullPath(Path.Combine(root, "..", "Resources", "server"));
        yield return Path.GetFullPath(Path.Combine(root, "..", "..", "MacOS"));
    }

    private static void Consider(List<string> found, string directory)
    {
        if (!Directory.Exists(directory))
            return;
        var server = File.Exists(Path.Combine(directory, HostFileName));
        var desktop = File.Exists(Path.Combine(directory, DesktopFileName));
        if (!server && !desktop)
            return;

        var full = TrimSlash(Path.GetFullPath(directory));
        if (found.Exists(existing => string.Equals(existing, full, PathComparison)))
            return;
        found.Add(full);
    }

    private static List<string> Distinct(IReadOnlyList<string> directories)
    {
        var list = new List<string>();
        foreach (var directory in directories)
        {
            var full = TrimSlash(Path.GetFullPath(directory));
            if (list.Exists(existing => string.Equals(existing, full, PathComparison)))
                continue;
            list.Add(full);
        }

        list.Sort(OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
        return list;
    }

    private static string TrimSlash(string path) =>
        path.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

    private static string Normalize(string path)
    {
        var trimmed = path.Trim().Trim('"');
        trimmed = TrimSlash(trimmed);
        try
        {
            return TrimSlash(Path.GetFullPath(trimmed));
        }
        catch (Exception)
        {
            return trimmed;
        }
    }

    private static string ShellSingleQuote(string value) =>
        "'" + value.Replace("'", "'\\''", StringComparison.Ordinal) + "'";

    private static StringComparison PathComparison =>
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    private static void BroadcastEnvironment()
    {
        if (!OperatingSystem.IsWindows())
            return;
        _ = NativeWindow.SendMessageTimeout(new IntPtr(0xffff), 0x001A, UIntPtr.Zero, "Environment", 2, 5000, out _);
    }

    private static class NativeWindow
    {
        [DllImport("user32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern IntPtr SendMessageTimeout(IntPtr hWnd, int msg, UIntPtr wParam, string lParam, int flags, int timeout, out UIntPtr result);
    }
}

public readonly record struct CommandPathResult(bool Changed, bool Ready, string Message);
