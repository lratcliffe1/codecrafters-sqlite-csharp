namespace codecrafters_sqlite.src.Helpers;

public static class VarintHelper
{
  public static (ulong value, int length) ReadVarint(Stream file)
  {
    // SQLite stores integers in a variable-length encoding (varint).
    // We rebuild that integer by reading one byte at a time and
    // appending bits into this accumulator.
    ulong value = 0;

    // The first 8 bytes contribute 7 bits each; the high bit says
    // "there are more bytes after this one".
    for (int i = 0; i < 8; i++)
    {
      // Read one byte from the stream (returns -1 on EOF).
      // We need to handle EOF explicitly to avoid silent bad reads.
      int rawByte = file.ReadByte();
      if (rawByte == -1)
        throw new EndOfStreamException();

      // Convert to unsigned byte so bit operations are clean.
      byte b = (byte)rawByte;
      // Shift existing bits left by 7 to make room,
      // then add the lower 7 bits from this byte.
      // The top bit is reserved as a "continue" flag.
      value = (value << 7) | (byte)(b & 0x7F);

      // If the high bit is not set, this is the final byte for the varint.
      if ((b & 0x80) == 0)
        return (value, i + 1);
    }

    // If we reach here, we already read 8 bytes with the "continue" flag set.
    // SQLite encodes the 9th byte as a full 8 bits (not 7).
    int lastRawByte = file.ReadByte();
    if (lastRawByte == -1)
      throw new EndOfStreamException();

    // Append the final 8 bits to the accumulator.
    value = (value << 8) | (ulong)(byte)lastRawByte;
    // This varint used all 9 bytes.
    return (value, 9);
  }

  public static ulong ReadVarint(ReadOnlySpan<byte> buffer, ref int offset)
  {
    // Same varint decoding as the stream version, but from a byte buffer.
    // The offset is advanced as bytes are consumed.
    ulong value = 0;

    // Read up to 8 bytes that contribute 7 bits each.
    for (int i = 0; i < 8; i++)
    {
      if (offset >= buffer.Length)
        throw new EndOfStreamException();

      // Grab the next byte and advance the offset.
      byte b = buffer[offset++];
      // Append the lower 7 bits into the accumulator.
      value = (value << 7) | (byte)(b & 0x7F);

      // If the high bit is not set, this was the last byte.
      if ((b & 0x80) == 0)
        return value;
    }

    if (offset >= buffer.Length)
      throw new EndOfStreamException();

    // The 9th byte contributes all 8 bits.
    value = (value << 8) | buffer[offset++];

    // Return the decoded value.
    return value;
  }
}