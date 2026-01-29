
using System.Text;
using codecrafters_sqlite.src.Classes;

namespace codecrafters_sqlite.src.Helpers;

public static class RecordHelper
{
  public static void PrintLeafRows(FileStream databaseFile, DatabaseHeader databaseHeader, TableData table, List<int> cellPointerArray, ParsedSelectQuery parsedQuery, int pageNumber, HashSet<long>? allowedRowIds = null)
  {
    var pageStart = PageHelper.GetPageStart(databaseHeader.PageSize, pageNumber);

    foreach (ushort pointer in cellPointerArray)
    {
      databaseFile.Seek(pageStart + pointer, SeekOrigin.Begin);

      var (_, _) = VarintHelper.ReadVarint(databaseFile);
      var (rowId, _) = VarintHelper.ReadVarint(databaseFile);

      if (allowedRowIds != null && !allowedRowIds.Contains((long)rowId))
        continue;

      // 1. Read the Record Header Size
      var (headerSize, headerSizeLen) = VarintHelper.ReadVarint(databaseFile);

      // 2. Read all the Serial Type Varints in the header
      List<ulong> serialTypes = ReadSerialTypes(databaseFile, headerSize, headerSizeLen);

      // 3. Read the columns from the body using the lengths
      int i = 0;
      List<string> selectedValues = [.. new string[parsedQuery.Selected.Count]];
      Dictionary<string, string> rowValues = [];

      foreach (var column in table.Columns)
      {
        ulong serialType = serialTypes[i++];
        int length = GetSerialTypeLength(serialType);
        bool isRowIdAlias = length == 0
          && column.IsPrimaryKey
          && string.Equals(column.Type, "integer", StringComparison.OrdinalIgnoreCase);

        if (isRowIdAlias)
        {
          string rowIdValue = rowId.ToString();
          rowValues.Add(column.Name, rowIdValue);

          var insertIndex = parsedQuery.Selected.IndexOf(column.Name);
          if (insertIndex != -1)
            selectedValues[insertIndex] = rowIdValue;

          continue;
        }

        string value = ReadSerialValue(databaseFile, serialType, length);

        rowValues.Add(column.Name, value);

        var selectedIndex = parsedQuery.Selected.IndexOf(column.Name);
        if (selectedIndex == -1)
          continue;

        selectedValues[selectedIndex] = value;
      }

      if (!parsedQuery.WherePredicate(rowValues))
        continue;

      Console.WriteLine(string.Join("|", selectedValues));
    }
  }

  public static List<Record> ReadSchemaRecords(FileStream file, List<int> cellPointers)
  {
    return cellPointers.Select(x => ReadSchemaRecord(file, x)).ToList();
  }

  public static Record ReadSchemaRecord(FileStream file, int cellOffset)
  {
    file.Seek(cellOffset, SeekOrigin.Begin);

    var (_, _) = VarintHelper.ReadVarint(file);
    var (_, _) = VarintHelper.ReadVarint(file); // Table B-Tree key

    var (headerSize, headerSizeLen) = VarintHelper.ReadVarint(file);
    int bytesToReadForTypes = (int)headerSize - headerSizeLen;
    byte[] typeBuffer = new byte[bytesToReadForTypes];
    file.ReadExactly(typeBuffer);

    List<ulong> serialTypeCodes = [];
    int offset = 0;
    while (offset < typeBuffer.Length)
    {
      serialTypeCodes.Add(VarintHelper.ReadVarint(typeBuffer, ref offset));
    }

    string[] columnValues = new string[5];

    for (int i = 0; i < serialTypeCodes.Count; i++)
    {
      int len = GetSerialTypeLength(serialTypeCodes[i]);
      byte[] data = new byte[len];
      if (len > 0) file.ReadExactly(data);

      if (i == 3) // rootpage column
        columnValues[i] = ReadBigEndianUnsignedInteger(data).ToString();
      else if (i < 5)
        columnValues[i] = Encoding.UTF8.GetString(data);
    }

    return new Record
    {
      Type = columnValues[0],
      Name = columnValues[1],
      TableName = columnValues[2],
      RootPage = int.Parse(columnValues[3]),
      Sql = columnValues[4]
    };
  }

  public static TableData ParseTableSchema(Record record)
  {
    List<string> sqlData = record.Sql
      .Split("(").Last()
      .Split(")").First()
      .Replace("\n", "")
      .Split(',')
      .ToList();

    List<Column> columns = [];

    foreach (var row in sqlData)
    {
      List<string> rowData = row.Trim().Split(" ").ToList();

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

  public static (string Key, long RowId) ReadIndexLeafEntry(FileStream file)
  {
    var (_, _) = VarintHelper.ReadVarint(file);
    List<string> values = ReadPayloadValues(file);

    if (values.Count < 2)
      throw new InvalidDataException("Index record did not contain key and rowid.");

    string key = values[0];
    if (!long.TryParse(values[^1], out long rowId))
      throw new InvalidDataException("Index rowid could not be parsed as integer.");

    return (key, rowId);
  }

  public static string ReadIndexKeyFromCell(FileStream file)
  {
    var (_, _) = VarintHelper.ReadVarint(file);
    List<string> values = ReadPayloadValues(file);

    if (values.Count == 0)
      throw new InvalidDataException("Index record did not contain a key.");

    return values[0];
  }

  private static List<string> ReadPayloadValues(FileStream file)
  {
    var (headerSize, headerSizeLen) = VarintHelper.ReadVarint(file);
    List<ulong> serialTypes = ReadSerialTypes(file, headerSize, headerSizeLen);

    List<string> values = [];
    foreach (var serialType in serialTypes)
    {
      int length = GetSerialTypeLength(serialType);
      values.Add(ReadSerialValue(file, serialType, length));
    }

    return values;
  }

  private static List<ulong> ReadSerialTypes(FileStream file, ulong headerSize, int headerSizeLen)
  {
    int bytesToRead = (int)headerSize - headerSizeLen;
    byte[] headerBuffer = new byte[bytesToRead];
    file.ReadExactly(headerBuffer);

    List<ulong> serialTypes = [];
    int offset = 0;
    while (offset < headerBuffer.Length)
    {
      serialTypes.Add(VarintHelper.ReadVarint(headerBuffer, ref offset));
    }

    return serialTypes;
  }

  private static int GetSerialTypeLength(ulong serialType)
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

  private static string ReadSerialValue(FileStream file, ulong serialType, int length)
  {
    if (serialType == 0)
      return string.Empty;

    if (serialType == 8)
      return "0";

    if (serialType == 9)
      return "1";

    if (serialType == 7)
    {
      byte[] data = new byte[8];
      file.ReadExactly(data);
      if (BitConverter.IsLittleEndian)
        Array.Reverse(data);
      double value = BitConverter.ToDouble(data, 0);
      return value.ToString();
    }

    if (serialType is >= 1 and <= 6)
    {
      byte[] data = new byte[length];
      file.ReadExactly(data);
      long value = ReadBigEndianSignedInteger(data);
      return value.ToString();
    }

    byte[] dataBytes = new byte[length];
    if (length > 0)
      file.ReadExactly(dataBytes);

    if (serialType >= 12 && serialType % 2 == 0)
      return Convert.ToHexString(dataBytes);

    return Encoding.UTF8.GetString(dataBytes);
  }

  private static long ReadBigEndianUnsignedInteger(ReadOnlySpan<byte> buffer)
  {
    long value = 0;
    foreach (byte b in buffer)
      value = (value << 8) | b;
    return value;
  }

  private static long ReadBigEndianSignedInteger(ReadOnlySpan<byte> buffer)
  {
    long value = ReadBigEndianUnsignedInteger(buffer);
    int bitCount = buffer.Length * 8;
    long signBit = 1L << (bitCount - 1);
    if ((value & signBit) != 0)
      value -= 1L << bitCount;
    return value;
  }
}