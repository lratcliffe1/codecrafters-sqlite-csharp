using static System.Buffers.Binary.BinaryPrimitives;

namespace codecrafters_sqlite.src.Helpers;

public static class CellPointerHelper
{
  public static List<int> ReadCellPointers(FileStream file, byte type, int pageNumber, uint pageSize, ushort cellCount)
  {
    int pageHeaderSize = (type == 0x02 || type == 0x05) ? 12 : 8;
    int fileHeaderSize = (pageNumber == 1) ? 100 : 0;

    long pageStart = (pageNumber - 1) * pageSize;
    long pointerArrayStart = pageStart + fileHeaderSize + pageHeaderSize;

    file.Seek(pointerArrayStart, SeekOrigin.Begin);

    byte[] buffer = new byte[cellCount * 2];
    file.ReadExactly(buffer, 0, cellCount * 2);

    List<int> pointers = [];

    for (var i = 0; i < cellCount; i++)
    {
      pointers.Add(ReadUInt16BigEndian(buffer.AsSpan(i * 2, 2)));
    }

    return pointers;
  }
}