using System.Text;
using codecrafters_sqlite.src.Classes;
using System.Buffers.Binary;

namespace codecrafters_sqlite.src;

public static class Helper
{
  public static DatabaseHeader ReadDatabaseHeader(FileStream file)
  {
    file.Seek(0, SeekOrigin.Begin);

    byte[] buffer = new byte[100];
    file.ReadExactly(buffer, 0, 100);

    ushort rawPageSize = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(16, 2));

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
      FileChangeCounter = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(24, 4)),
      DatabaseSizeInPages = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(28, 4)),
      FirstFreelistTrunkPage = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(32, 4)),
      TotalFreelistPages = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(36, 4)),
      SchemaCookie = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(40, 4)),
      SchemaFormatNumber = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(44, 4)),
      DefaultPageCacheSize = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(48, 4)),
      LargestRootBTreePage = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(52, 4)),
      DatabaseTextEncoding = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(56, 4)),
      UserVersion = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(60, 4)),
      IncrementalVacuumMode = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(64, 4)),
      ApplicationId = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(68, 4)),
      VersionValidFor = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(92, 4)),
      SqliteVersionNumber = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(96, 4))
    };
  }

  public static BTreePageHeader ReadPageHeader(FileStream file, int pageNumber, uint pageSize)
  {
    // Page 1 starts at 100, all others start at 0
    int headerOffsetInsidePage = (pageNumber == 1) ? 100 : 0;
    long pageStart = (pageNumber - 1) * pageSize;

    file.Seek(pageStart + headerOffsetInsidePage, SeekOrigin.Begin);

    // Interior pages have a 12-byte header, Leaf pages have 8 bytes
    // We read 12 to be safe, then check the type
    byte[] buffer = new byte[12];
    file.ReadExactly(buffer, 0, 12);

    byte type = buffer[0];
    ushort rawContentStart = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(5, 2));

    return new BTreePageHeader
    {
      PageType = type,
      FirstFreeblock = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(1, 2)),
      CellCount = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(3, 2)),
      CellContentStart = rawContentStart == 0 ? 65536u : rawContentStart,
      FragmentedFreeBytes = buffer[7],
      RightMostPointer = (type == 0x02 || type == 0x05) ? BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(8, 4)) : null
    };
  }

  public static List<int> GetCellPointerArray(FileStream file, byte type, int pageNumber, uint pageSize, ushort cellCount)
  {
    int pageHeaderSize = (type == 0x02 || type == 0x05) ? 12 : 8;
    int fileHeaderSize = (pageNumber == 1) ? 100 : 0;

    long pageStart = (pageNumber - 1) * pageSize;
    long pointerArrayStart = pageStart + fileHeaderSize + pageHeaderSize;

    file.Seek(pointerArrayStart, SeekOrigin.Begin);

    byte[] buffer = new byte[cellCount * 2];
    file.ReadExactly(buffer, 0, cellCount * 2);

    List<int> pointerArray = [];

    for (var i = 0; i < cellCount; i++)
    {
      pointerArray.Add(BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(i * 2, 2)));
    }

    return pointerArray;
  }

  public static List<Record> GetRecordData(FileStream file, List<int> pointerArrayStarts)
  {
    return pointerArrayStarts.Select(x => GetRecordData(file, x)).ToList();
  }

  public static Record GetRecordData(FileStream file, int pointerArrayStart)
  {
    file.Seek(pointerArrayStart, SeekOrigin.Begin);

    byte[] buffer = new byte[3];
    file.ReadExactly(buffer, 0, 3);

    _ = buffer[0];
    _ = buffer[1];
    var sizeOfHeaderReccord = buffer[2];

    buffer = new byte[sizeOfHeaderReccord];
    file.ReadExactly(buffer, 0, sizeOfHeaderReccord);

    List<int> lengths = [];

    for (var i = 0; i < sizeOfHeaderReccord; i++)
    {
      if (buffer[i] == 1)
      {
        lengths.Add(2);
        break;
      }

      lengths.Add((buffer[i] - 13) / 2);
    }

    file.Seek(pointerArrayStart + sizeOfHeaderReccord + 2, SeekOrigin.Begin);

    string type = "";
    string name = "";
    string tableName = "";
    byte rootPage = 0;

    foreach (var len in lengths)
    {
      buffer = new byte[len];
      file.ReadExactly(buffer, 0, len);

      if (type == "")
        type = Encoding.UTF8.GetString(buffer, 0, len);
      else if (name == "")
        name = Encoding.UTF8.GetString(buffer, 0, len);
      else if (tableName == "")
        tableName = Encoding.UTF8.GetString(buffer, 0, len);
      else
        rootPage = buffer[0];
    }

    return new Record
    {
      Type = type,
      Name = name,
      TableName = tableName,
      RootPage = rootPage,
    };
  }

  public static void SeeData<T>(T obj)
  {
    foreach (var prop in typeof(T).GetProperties())
    {
      string name = prop.Name;
      object? value = prop.GetValue(obj);

      Console.Error.WriteLine($"{name,-25}: {value}");
    }
  }
}