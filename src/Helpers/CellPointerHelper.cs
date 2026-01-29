using static System.Buffers.Binary.BinaryPrimitives;

namespace codecrafters_sqlite.src.Helpers;

public static class CellPointerHelper
{
  public static List<int> ReadCellPointers(FileStream file, byte type, int pageNumber, uint pageSize, ushort cellCount)
  {
    int pageHeaderSize = SqliteConstants.GetPageHeaderSize(type);
    int fileHeaderSize = (pageNumber == SqliteConstants.SchemaPageNumber) ? SqliteConstants.SchemaHeaderSize : 0;

    long pageStart = PageHelper.GetPageStart(pageSize, pageNumber);
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