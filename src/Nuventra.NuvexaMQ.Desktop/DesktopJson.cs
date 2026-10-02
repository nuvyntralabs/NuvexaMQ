using System.Text.Json.Serialization;

namespace Nuventra.NuvexaMQ.Desktop;

[JsonSourceGenerationOptions(WriteIndented = true, PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(DesktopSettings))]
internal sealed partial class DesktopJsonContext : JsonSerializerContext;
