namespace codecrafters_sqlite.src.Helpers;

public class VarintHelper()
{
  public static (ulong value, int length) ReadVarint(FileStream file)
  {
    ulong value = 0;

    for (int i = 0; i < 8; i++)
    {
      int rawByte = file.ReadByte();
      if (rawByte == -1)
        throw new EndOfStreamException();

      byte b = (byte)rawByte;
      value = (value << 7) | (byte)(b & 0x7F);

      if ((b & 0x80) == 0)
        return (value, i + 1);
    }

    int lastRawByte = file.ReadByte();
    if (lastRawByte == -1)
      throw new EndOfStreamException();

    value = (value << 8) | (ulong)(byte)lastRawByte;
    return (value, 9);
  }

  public static ulong ReadVarint(ReadOnlySpan<byte> buffer, ref int offset)
  {
    ulong value = 0;

    for (int i = 0; i < 8 && offset < buffer.Length; i++)
    {
      byte b = buffer[offset++];
      value = (value << 7) | (byte)(b & 0x7F);

      if ((b & 0x80) == 0)
        return value;
    }

    if (offset < buffer.Length)
      value = (value << 8) | buffer[offset++];

    return value;
  }

}