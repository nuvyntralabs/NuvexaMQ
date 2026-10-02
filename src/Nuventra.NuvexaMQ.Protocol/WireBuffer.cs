using System.Buffers.Binary;
using System.Text;

namespace Nuventra.NuvexaMQ.Protocol;

public sealed class WireBuffer
{
    private byte[] _buffer;
    private int _length;
    private int _position;

    private WireBuffer(byte[] buffer, int length, bool reading)
    {
        _buffer = buffer;
        _length = length;
        _position = reading ? 0 : length;
    }

    public static WireBuffer Writer() => new(new byte[128], 0, reading: false);

    public static WireBuffer Reader(ReadOnlyMemory<byte> payload) => new(payload.ToArray(), payload.Length, reading: true);

    public byte[] ToArray() => _buffer.AsSpan(0, _length).ToArray();

    public void WriteByte(byte value)
    {
        Ensure(1);
        _buffer[_length++] = value;
    }

    public void WriteUInt16(ushort value)
    {
        Ensure(2);
        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.AsSpan(_length), value);
        _length += 2;
    }

    public void WriteInt32(int value)
    {
        Ensure(4);
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.AsSpan(_length), value);
        _length += 4;
    }

    public void WriteInt64(long value)
    {
        Ensure(8);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer.AsSpan(_length), value);
        _length += 8;
    }

    public void WriteString(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        if (bytes.Length > ushort.MaxValue)
            throw new InvalidDataException("String is too long.");
        WriteUInt16((ushort)bytes.Length);
        Ensure(bytes.Length);
        bytes.CopyTo(_buffer.AsSpan(_length));
        _length += bytes.Length;
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        WriteInt32(value.Length);
        Ensure(value.Length);
        value.CopyTo(_buffer.AsSpan(_length));
        _length += value.Length;
    }

    public byte ReadByte()
    {
        Require(1);
        return _buffer[_position++];
    }

    public ushort ReadUInt16()
    {
        Require(2);
        var value = BinaryPrimitives.ReadUInt16LittleEndian(_buffer.AsSpan(_position));
        _position += 2;
        return value;
    }

    public int ReadInt32()
    {
        Require(4);
        var value = BinaryPrimitives.ReadInt32LittleEndian(_buffer.AsSpan(_position));
        _position += 4;
        return value;
    }

    public long ReadInt64()
    {
        Require(8);
        var value = BinaryPrimitives.ReadInt64LittleEndian(_buffer.AsSpan(_position));
        _position += 8;
        return value;
    }

    public string ReadString()
    {
        var length = ReadUInt16();
        Require(length);
        var value = Encoding.UTF8.GetString(_buffer, _position, length);
        _position += length;
        return value;
    }

    public byte[] ReadBytes()
    {
        var length = ReadInt32();
        if (length < 0)
            throw new InvalidDataException("Byte length is negative.");
        Require(length);
        var value = _buffer.AsSpan(_position, length).ToArray();
        _position += length;
        return value;
    }

    public bool HasRemaining => _position < _length;

    private void Ensure(int count)
    {
        if (_length + count <= _buffer.Length)
            return;
        var grown = new byte[Math.Max(_buffer.Length * 2, _length + count)];
        _buffer.AsSpan(0, _length).CopyTo(grown);
        _buffer = grown;
    }

    private void Require(int count)
    {
        if (_position + count > _length)
            throw new InvalidDataException("Frame ended early.");
    }
}
