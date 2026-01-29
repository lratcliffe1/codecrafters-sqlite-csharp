
using System.Text;
using codecrafters_sqlite.src.Classes;

namespace codecrafters_sqlite.src.Helpers;

public class RecordHelper()
{
  public static void PrintRecordValues(
    FileStream databaseFile,
    DatabaseHeader databaseHeader,
    TableData table,
    List<int> cellPointerArray,
    ParsedInput parsedInput,
    int pageNumber,
    HashSet<long>? allowedRowIds = null)
  {
    var pageStart = databaseHeader.PageSize * (pageNumber - 1);

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
      List<string> output = [.. new string[parsedInput.Selected.Count]];
      Dictionary<string, string> keyValuePairs = [];

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
          keyValuePairs.Add(column.Name, rowIdValue);

          var insertIndex = parsedInput.Selected.IndexOf(column.Name);
          if (insertIndex != -1)
            output[insertIndex] = rowIdValue;

          continue;
        }

        string value = ReadSerialTypeValue(databaseFile, serialType, length);

        keyValuePairs.Add(column.Name, value);

        var selectedIndex = parsedInput.Selected.IndexOf(column.Name);
        if (selectedIndex == -1)
          continue;

        output[selectedIndex] = value;
      }

      if (!parsedInput.Conditional(keyValuePairs))
        continue;

      Console.WriteLine(string.Join("|", output));
    }
  }

  public static List<Record> GetRecordData(FileStream file, List<int> pointerArrayStarts)
  {
    return pointerArrayStarts.Select(x => GetRecordData(file, x)).ToList();
  }

  public static Record GetRecordData(FileStream file, int cellOffset)
  {
    file.Seek(cellOffset, SeekOrigin.Begin);

    var (totalPayloadSize, _) = VarintHelper.ReadVarint(file);
    var (rowId, _) = VarintHelper.ReadVarint(file); // Table B-Tree key

    var (headerSize, headerSizeLen) = VarintHelper.ReadVarint(file);
    int bytesToReadForTypes = (int)headerSize - headerSizeLen;
    byte[] typeBuffer = new byte[bytesToReadForTypes];
    file.ReadExactly(typeBuffer);

    List<ulong> serialTypes = [];
    int offset = 0;
    while (offset < typeBuffer.Length)
    {
      serialTypes.Add(VarintHelper.ReadVarint(typeBuffer, ref offset));
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

  public static TableData ParceTableData(Record record)
  {
    List<string> sqlData = record.SQL
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
    List<string> values = ReadRecordPayloadValues(file);

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
    List<string> values = ReadRecordPayloadValues(file);

    if (values.Count == 0)
      throw new InvalidDataException("Index record did not contain a key.");

    return values[0];
  }

  private static List<string> ReadRecordPayloadValues(FileStream file)
  {
    var (headerSize, headerSizeLen) = VarintHelper.ReadVarint(file);
    List<ulong> serialTypes = ReadSerialTypes(file, headerSize, headerSizeLen);

    List<string> values = [];
    foreach (var serialType in serialTypes)
    {
      int length = GetSerialTypeLength(serialType);
      values.Add(ReadSerialTypeValue(file, serialType, length));
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

  private static string ReadSerialTypeValue(FileStream file, ulong serialType, int length)
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

  private static long ReadBigEndianInteger(ReadOnlySpan<byte> buffer)
  {
    long value = 0;
    foreach (byte b in buffer)
      value = (value << 8) | b;
    return value;
  }

  private static long ReadBigEndianSignedInteger(ReadOnlySpan<byte> buffer)
  {
    long value = ReadBigEndianInteger(buffer);
    int bitCount = buffer.Length * 8;
    long signBit = 1L << (bitCount - 1);
    if ((value & signBit) != 0)
      value -= 1L << bitCount;
    return value;
  }
}