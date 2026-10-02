using System.Text.Json;

namespace Nuventra.NuvexaMQ.Desktop;

public sealed class DesktopSettings
{
    public int BrokerPort { get; set; } = 5761;

    public int HealthPort { get; set; } = 5762;

    public int AdminHttpPort { get; set; } = 5763;

    public int AdminHttpsPort { get; set; } = 5764;

    public string DataDir { get; set; } = DefaultDataDir();

    public bool Verbose { get; set; }

    public bool EnableLogging { get; set; } = true;

    public string LogsDir { get; set; } = DefaultLogsDir();

    public static string SettingsPath { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "NuvexaMQ",
        "desktop.json");

    public static string DefaultDataDir() => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "NuvexaMQ",
        "data");

    public static string DefaultLogsDir() => Path.Combine(DefaultDataDir(), "logs");

    public static DesktopSettings Load()
    {
        try
        {
            if (!File.Exists(SettingsPath))
                return new DesktopSettings();
            return JsonSerializer.Deserialize(File.ReadAllText(SettingsPath), DesktopJsonContext.Default.DesktopSettings) ?? new DesktopSettings();
        }
        catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
        {
            return new DesktopSettings();
        }
    }

    public void Save()
    {
        var directory = Path.GetDirectoryName(SettingsPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);
        File.WriteAllText(SettingsPath, JsonSerializer.Serialize(this, DesktopJsonContext.Default.DesktopSettings));
    }
}
