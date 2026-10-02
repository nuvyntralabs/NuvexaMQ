using System.Buffers.Binary;
using System.Text;

namespace Nuventra.NuvexaMQ.Engine;

internal static class LogFormat
{
    public const int HeaderSize = 48;
    public const byte RecordMagic = 0xA5;
    public const byte RecordVersion = 1;
    public const int IndexStride = 4096;
    public static ReadOnlySpan<byte> SegmentMagic => "NMQ1"u8;
    public static ReadOnlySpan<byte> IndexMagic => "NMI1"u8;

    public static void WriteSegmentHeader(Stream stream, ulong epoch, long baseOffset)
    {
        Span<byte> header = stackalloc byte[HeaderSize];
        SegmentMagic.CopyTo(header);
        BinaryPrimitives.WriteUInt16LittleEndian(header[4..], 1);
        BinaryPrimitives.WriteUInt64LittleEndian(header[8..], epoch);
        BinaryPrimitives.WriteInt64LittleEndian(header[16..], baseOffset);
        stream.Write(header);
    }

    public static bool TryReadSegmentHeader(Stream stream, out ulong epoch, out long baseOffset)
    {
        epoch = 0;
        baseOffset = 0;
        Span<byte> header = stackalloc byte[HeaderSize];
        var read = ReadExact(stream, header);
        if (read != HeaderSize || !header[..4].SequenceEqual(SegmentMagic) || BinaryPrimitives.ReadUInt16LittleEndian(header[4..]) != 1)
            return false;

        epoch = BinaryPrimitives.ReadUInt64LittleEndian(header[8..]);
        baseOffset = BinaryPrimitives.ReadInt64LittleEndian(header[16..]);
        return true;
    }

    public static byte[] Encode(long offset, long timestampUnixMs, ReadOnlySpan<byte> key, string subject, IReadOnlyList<MessageHeader> headers, ReadOnlySpan<byte> payload)
    {
        var subjectBytes = Encoding.UTF8.GetBytes(subject);
        var size = 36 + key.Length + subjectBytes.Length + payload.Length;
        foreach (var header in headers)
            size += 2 + Encoding.UTF8.GetByteCount(header.Name) + 4 + header.Value.Length;

        var buffer = new byte[size];
        buffer[0] = RecordMagic;
        buffer[1] = RecordVersion;
        var cursor = 8;
        WriteInt64(buffer, ref cursor, offset);
        WriteInt64(buffer, ref cursor, timestampUnixMs);
        WriteInt32(buffer, ref cursor, key.Length);
        key.CopyTo(buffer.AsSpan(cursor));
        cursor += key.Length;
        WriteUInt16(buffer, ref cursor, (ushort)subjectBytes.Length);
        subjectBytes.CopyTo(buffer.AsSpan(cursor));
        cursor += subjectBytes.Length;
        WriteUInt16(buffer, ref cursor, (ushort)headers.Count);
        foreach (var header in headers)
        {
            var nameBytes = Encoding.UTF8.GetBytes(header.Name);
            WriteUInt16(buffer, ref cursor, (ushort)nameBytes.Length);
            nameBytes.CopyTo(buffer.AsSpan(cursor));
            cursor += nameBytes.Length;
            WriteInt32(buffer, ref cursor, header.Value.Length);
            header.Value.CopyTo(buffer.AsSpan(cursor));
            cursor += header.Value.Length;
        }

        WriteInt32(buffer, ref cursor, payload.Length);
        payload.CopyTo(buffer.AsSpan(cursor));
        var crc = Crc32.Compute(buffer.AsSpan(8));
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(4), crc);
        return buffer;
    }

    public static ScanStop Scan(Stream stream, int maxMessageBytes, List<IndexEntry> index, out long lastOffset, out long maxTimestamp, out int recordCount)
    {
        lastOffset = -1;
        maxTimestamp = 0;
        recordCount = 0;
        index.Clear();
        if (stream.Length < HeaderSize)
            return ScanStop.TornAt(0);

        stream.Position = 0;
        if (!TryReadSegmentHeader(stream, out _, out _))
            return ScanStop.TornAt(0);

        long good = HeaderSize;
        long lastIndexed = -IndexStride;
        while (stream.Position < stream.Length)
        {
            var start = stream.Position;
            if (!TryReadRecord(stream, maxMessageBytes, out var record))
                return ScanStop.TornAt(start);

            recordCount++;
            lastOffset = record.Offset;
            if (record.TimestampUnixMs > maxTimestamp)
                maxTimestamp = record.TimestampUnixMs;
            if (index.Count == 0 || start - lastIndexed >= IndexStride)
            {
                index.Add(new IndexEntry(record.Offset, start));
                lastIndexed = start;
            }

            good = stream.Position;
        }

        return ScanStop.Clean(good);
    }

    public static bool TryReadRecord(Stream stream, int maxMessageBytes, out StoredRecord record)
    {
        record = null!;
        var start = stream.Position;
        if (stream.Length - start < 36)
            return false;

        using var raw = new MemoryStream();
        if (!Append(stream, raw, 8))
        {
            stream.Position = start;
            return false;
        }

        var prefix = raw.GetBuffer();
        if (prefix[0] != RecordMagic || prefix[1] != RecordVersion)
        {
            stream.Position = start;
            return false;
        }

        var storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(raw.GetBuffer().AsSpan(4));
        if (!Append(stream, raw, 16) || !TryAppendLength(stream, raw, maxMessageBytes, out var keyLength) || !Append(stream, raw, keyLength))
        {
            stream.Position = start;
            return false;
        }

        if (!TryAppendUInt16(stream, raw, 256, out var subjectLength) || !Append(stream, raw, subjectLength))
        {
            stream.Position = start;
            return false;
        }

        if (!TryAppendUInt16(stream, raw, 64, out var headerCount))
        {
            stream.Position = start;
            return false;
        }

        for (var i = 0; i < headerCount; i++)
        {
            if (!TryAppendUInt16(stream, raw, 128, out var nameLength) || !Append(stream, raw, nameLength))
            {
                stream.Position = start;
                return false;
            }

            if (!TryAppendLength(stream, raw, maxMessageBytes, out var valueLength) || !Append(stream, raw, valueLength))
            {
                stream.Position = start;
                return false;
            }
        }

        if (!TryAppendLength(stream, raw, maxMessageBytes, out var payloadLength) || !Append(stream, raw, payloadLength))
        {
            stream.Position = start;
            return false;
        }

        var bytes = raw.ToArray();
        if (Crc32.Compute(bytes.AsSpan(8)) != storedCrc)
        {
            stream.Position = start;
            return false;
        }

        var cursor = 8;
        var offset = ReadInt64(bytes, ref cursor);
        var timestamp = ReadInt64(bytes, ref cursor);
        var keyLen = ReadInt32(bytes, ref cursor);
        var key = bytes.AsSpan(cursor, keyLen).ToArray();
        cursor += keyLen;
        var subjectLen = ReadUInt16(bytes, ref cursor);
        var subject = Encoding.UTF8.GetString(bytes, cursor, subjectLen);
        cursor += subjectLen;
        var headersInRecord = ReadUInt16(bytes, ref cursor);
        var headers = new MessageHeader[headersInRecord];
        for (var i = 0; i < headersInRecord; i++)
        {
            var nameLen = ReadUInt16(bytes, ref cursor);
            var name = Encoding.UTF8.GetString(bytes, cursor, nameLen);
            cursor += nameLen;
            var valueLen = ReadInt32(bytes, ref cursor);
            var value = bytes.AsSpan(cursor, valueLen).ToArray();
            cursor += valueLen;
            headers[i] = new MessageHeader(name, value);
        }

        var payloadLen = ReadInt32(bytes, ref cursor);
        record = new StoredRecord
        {
            Offset = offset,
            TimestampUnixMs = timestamp,
            Subject = subject,
            Key = key,
            Headers = headers,
            Payload = bytes.AsSpan(cursor, payloadLen).ToArray()
        };
        return true;
    }

    public static void WriteIndex(string path, IReadOnlyList<IndexEntry> index)
    {
        var tmp = path + ".tmp";
        using (var stream = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            Span<byte> magic = stackalloc byte[4];
            IndexMagic.CopyTo(magic);
            stream.Write(magic);
            Span<byte> numbers = stackalloc byte[8];
            BinaryPrimitives.WriteUInt16LittleEndian(numbers, 1);
            BinaryPrimitives.WriteUInt16LittleEndian(numbers[2..], 0);
            BinaryPrimitives.WriteInt32LittleEndian(numbers[4..], index.Count);
            stream.Write(numbers);
            Span<byte> row = stackalloc byte[16];
            foreach (var entry in index)
            {
                BinaryPrimitives.WriteInt64LittleEndian(row, entry.Offset);
                BinaryPrimitives.WriteInt64LittleEndian(row[8..], entry.Position);
                stream.Write(row);
            }

            stream.Flush(true);
        }

        File.Move(tmp, path, overwrite: true);
    }

    private static bool Append(Stream stream, MemoryStream raw, int length)
    {
        if (length == 0)
            return true;
        if (length < 0 || stream.Length - stream.Position < length)
            return false;

        var bytes = new byte[length];
        if (ReadExact(stream, bytes) != length)
            return false;

        raw.Write(bytes);
        return true;
    }

    private static bool TryAppendLength(Stream stream, MemoryStream raw, int max, out int length)
    {
        length = 0;
        var before = raw.Length;
        if (!Append(stream, raw, 4))
            return false;

        length = BinaryPrimitives.ReadInt32LittleEndian(raw.GetBuffer().AsSpan((int)before));
        return length >= 0 && length <= max;
    }

    private static bool TryAppendUInt16(Stream stream, MemoryStream raw, int max, out int value)
    {
        value = 0;
        var before = raw.Length;
        if (!Append(stream, raw, 2))
            return false;

        value = BinaryPrimitives.ReadUInt16LittleEndian(raw.GetBuffer().AsSpan((int)before));
        return value <= max;
    }

    private static int ReadExact(Stream stream, Span<byte> buffer)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = stream.Read(buffer[offset..]);
            if (read == 0)
                break;
            offset += read;
        }

        return offset;
    }

    private static void WriteInt64(byte[] buffer, ref int cursor, long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(cursor), value);
        cursor += 8;
    }

    private static void WriteInt32(byte[] buffer, ref int cursor, int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(cursor), value);
        cursor += 4;
    }

    private static void WriteUInt16(byte[] buffer, ref int cursor, ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(cursor), value);
        cursor += 2;
    }

    private static long ReadInt64(byte[] buffer, ref int cursor)
    {
        var value = BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(cursor));
        cursor += 8;
        return value;
    }

    private static int ReadInt32(byte[] buffer, ref int cursor)
    {
        var value = BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(cursor));
        cursor += 4;
        return value;
    }

    private static ushort ReadUInt16(byte[] buffer, ref int cursor)
    {
        var value = BinaryPrimitives.ReadUInt16LittleEndian(buffer.AsSpan(cursor));
        cursor += 2;
        return value;
    }
}

internal readonly record struct IndexEntry(long Offset, long Position);

internal readonly record struct ScanStop(bool Torn, long GoodLength)
{
    public static ScanStop Clean(long goodLength) => new(false, goodLength);

    public static ScanStop TornAt(long goodLength) => new(true, goodLength);
}
