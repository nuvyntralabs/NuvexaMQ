using System.Text;

namespace Nuventra.NuvexaMQ.Engine;

public static class SubjectFilter
{
    public static bool Matches(string filter, string subject)
    {
        var filterTokens = filter.Split('.');
        var subjectTokens = subject.Split('.');
        var filterIndex = 0;
        var subjectIndex = 0;
        while (filterIndex < filterTokens.Length)
        {
            var token = filterTokens[filterIndex];
            if (token == ">")
                return filterIndex == filterTokens.Length - 1 && subjectIndex < subjectTokens.Length;

            if (subjectIndex >= subjectTokens.Length)
                return false;

            if (token != "*" && !string.Equals(token, subjectTokens[subjectIndex], StringComparison.Ordinal))
                return false;

            filterIndex++;
            subjectIndex++;
        }

        return subjectIndex == subjectTokens.Length;
    }

    internal static void ValidateName(string name, string kind)
    {
        if (name.Length is < 1 or > 128 || name.Any(c => !IsNameChar(c)))
            throw new NuvexaMqException(NuvexaMqError.Invalid, $"{kind} name '{name}' is not valid.");
    }

    internal static void ValidateSubject(string subject)
    {
        if (subject.Length is < 1 or > 256)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "Subject length must be 1 to 256 characters.");

        foreach (var token in subject.Split('.'))
        {
            if (token.Length == 0 || token is "*" or ">" || token.Any(char.IsWhiteSpace))
                throw new NuvexaMqException(NuvexaMqError.Invalid, $"Subject '{subject}' is not a publishable name.");
        }
    }

    internal static void ValidateFilter(string filter)
    {
        if (filter.Length is < 1 or > 256)
            throw new NuvexaMqException(NuvexaMqError.Invalid, "A subject filter must be 1 to 256 characters.");

        var tokens = filter.Split('.');
        for (var i = 0; i < tokens.Length; i++)
        {
            var token = tokens[i];
            if (token.Length == 0)
                throw new NuvexaMqException(NuvexaMqError.Invalid, $"Filter '{filter}' has an empty token.");

            if (token == ">")
            {
                if (i != tokens.Length - 1)
                    throw new NuvexaMqException(NuvexaMqError.Invalid, $"Filter '{filter}' must end at '>'.");
                continue;
            }

            if (token == "*")
                continue;

            if (token.Any(c => c is '*' or '>' || char.IsWhiteSpace(c)))
                throw new NuvexaMqException(NuvexaMqError.Invalid, $"Filter '{filter}' has an invalid token.");
        }
    }

    internal static int PartitionFor(ReadOnlySpan<byte> key, int count, ref int roundRobin)
    {
        if (count <= 1)
            return 0;

        if (key.IsEmpty)
        {
            var ticket = Interlocked.Increment(ref roundRobin);
            var index = ticket - 1;
            if (index < 0)
                index = 0;
            return index % count;
        }

        uint hash = 2166136261;
        foreach (var b in key)
        {
            hash ^= b;
            hash *= 16777619;
        }

        return (int)(hash % (uint)count);
    }

    internal static byte[] EncodeKey(string? key) =>
        string.IsNullOrEmpty(key) ? [] : Encoding.UTF8.GetBytes(key);

    private static bool IsNameChar(char c) =>
        char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-' or '$';
}
