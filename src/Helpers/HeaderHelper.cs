using System.Text;
using codecrafters_sqlite.src.Classes;
using static System.Buffers.Binary.BinaryPrimitives;

namespace codecrafters_sqlite.src.Helpers;

public class HeaderHelper()
{
  public static DatabaseHeader ReadDatabaseHeader(FileStream file)
  {
    file.Seek(0, SeekOrigin.Begin);

    byte[] buffer = new byte[100];
    file.ReadExactly(buffer, 0, 100);

    ushort rawPageSize = ReadUInt16BigEndian(buffer.AsSpan(16, 2));

    return new DatabaseHeader
    {
      MagicHeaderString = Encoding.UTF8.GetString(buffer, 0, 16),
      PageSize = rawPageSize == 1 ? 65536u : rawPageSize,
      FileFormatWriteVersion = buffer[18],
      FileFormatReadVersion = buffer[19],
      ReservedSpace = buffer[20],
      MaxPayloadFraction = buffer[21],
      MinPayloadFraction = buffer[22],
      LeafPayloadFraction = buffer[23],
      FileChangeCounter = ReadUInt32BigEndian(buffer.AsSpan(24, 4)),
      DatabaseSizeInPages = ReadUInt32BigEndian(buffer.AsSpan(28, 4)),
      FirstFreelistTrunkPage = ReadUInt32BigEndian(buffer.AsSpan(32, 4)),
      TotalFreelistPages = ReadUInt32BigEndian(buffer.AsSpan(36, 4)),
      SchemaCookie = ReadUInt32BigEndian(buffer.AsSpan(40, 4)),
      SchemaFormatNumber = ReadUInt32BigEndian(buffer.AsSpan(44, 4)),
      DefaultPageCacheSize = ReadUInt32BigEndian(buffer.AsSpan(48, 4)),
      LargestRootBTreePage = ReadUInt32BigEndian(buffer.AsSpan(52, 4)),
      DatabaseTextEncoding = ReadUInt32BigEndian(buffer.AsSpan(56, 4)),
      UserVersion = ReadUInt32BigEndian(buffer.AsSpan(60, 4)),
      IncrementalVacuumMode = ReadUInt32BigEndian(buffer.AsSpan(64, 4)),
      ApplicationId = ReadUInt32BigEndian(buffer.AsSpan(68, 4)),
      VersionValidFor = ReadUInt32BigEndian(buffer.AsSpan(92, 4)),
      SqliteVersionNumber = ReadUInt32BigEndian(buffer.AsSpan(96, 4))
    };
  }

  public static BTreePageHeader ReadPageHeader(FileStream file, int pageNumber, uint pageSize)
  {
    // Page 1 starts at 100, all others start at 0
    int headerOffsetInsidePage = (pageNumber == 1) ? 100 : 0;
    long pageStart = (pageNumber - 1) * pageSize;

    file.Seek(pageStart + headerOffsetInsidePage, SeekOrigin.Begin);

    // Interior pages have a 12-byte header, Leaf pages have 8 bytes
    // internal page (points to other pages), leaf page (contains data)
    // We read 12 to be safe, then check the type
    byte[] buffer = new byte[12];
    file.ReadExactly(buffer, 0, 12);

    byte type = buffer[0];
    ushort rawContentStart = ReadUInt16BigEndian(buffer.AsSpan(5, 2));

    return new BTreePageHeader
    {
      PageType = type,
      FirstFreeblock = ReadUInt16BigEndian(buffer.AsSpan(1, 2)),
      CellCount = ReadUInt16BigEndian(buffer.AsSpan(3, 2)),
      CellContentStart = rawContentStart == 0 ? 65536u : rawContentStart,
      FragmentedFreeBytes = buffer[7],
      RightMostPointer = (type == 0x02 || type == 0x05) ? ReadUInt32BigEndian(buffer.AsSpan(8, 4)) : null
    };
  }

}