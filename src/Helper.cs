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
    // internal page (points to other pages), leaf page (contains data)
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

  public static Record GetRecordData(FileStream file, int cellOffset)
  {
    file.Seek(cellOffset, SeekOrigin.Begin);

    var (totalPayloadSize, _) = ReadVarint(file);
    var (rowId, _) = ReadVarint(file); // Table B-Tree key

    var (headerSize, headerSizeLen) = ReadVarint(file);
    int bytesToReadForTypes = (int)headerSize - headerSizeLen;
    byte[] typeBuffer = new byte[bytesToReadForTypes];
    file.ReadExactly(typeBuffer);

    List<ulong> serialTypes = [];
    int offset = 0;
    while (offset < typeBuffer.Length)
    {
      serialTypes.Add(ReadVarint(typeBuffer, ref offset));
    }

    string[] results = new string[5];

    for (int i = 0; i < serialTypes.Count; i++)
    {
      int len = GetSerialTypeLength(serialTypes[i]);
      byte[] data = new byte[len];
      if (len > 0) file.ReadExactly(data);

      if (i == 3) // rootpage column
        results[i] = ReadBigEndianInteger(data).ToString();
      else if (i < 5)
        results[i] = Encoding.UTF8.GetString(data);
    }

    return new Record
    {
      Type = results[0],
      Name = results[1],
      TableName = results[2],
      RootPage = int.Parse(results[3]),
      SQL = results[4]
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

  public static int GetSerialTypeLength(ulong serialType)
  {
    return serialType switch
    {
      0 => 0,
      1 => 1,
      2 => 2,
      3 => 3,
      4 => 4,
      5 => 6,
      6 => 8,
      7 => 8,
      8 => 0,
      9 => 0,
      _ => serialType >= 12
        ? (int)((serialType - (serialType % 2 == 0 ? 12u : 13u)) / 2)
        : 0
    };
  }

  private static long ReadBigEndianInteger(ReadOnlySpan<byte> buffer)
  {
    long value = 0;
    foreach (byte b in buffer)
      value = (value << 8) | b;
    return value;
  }

  public static TableData ParceTableData(Record record)
  {
    List<string> sqlData = record.SQL
      .Split("(").Last()
      .Split(")").First()
      .Replace("\n", "")
      .Split(',').ToList();

    List<Column> columns = [];

    foreach (var row in sqlData)
    {
      List<string> rowData = row.Split(" ").ToList();

      columns.Add(new Column
      {
        Name = rowData[0].Trim(),
        Type = rowData[1].Trim(),
        IsPrimaryKey = rowData.Count > 3 && rowData[2] == "primary" && rowData[3] == "key",
      });
    }

    return new TableData()
    {
      Type = record.Type,
      Name = record.Name,
      TableName = record.TableName,
      RootPage = record.RootPage,
      Columns = columns,
    };
  }
}