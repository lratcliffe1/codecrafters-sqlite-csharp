
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
    ParsedInput parsedInput)
  {
    var pageStart = databaseHeader.PageSize * (table.RootPage - 1);

    foreach (ushort pointer in cellPointerArray)
    {
      databaseFile.Seek(pageStart + pointer, SeekOrigin.Begin);

      var (payloadSize, _) = VarintHelper.ReadVarint(databaseFile);
      var (rowId, _) = VarintHelper.ReadVarint(databaseFile);

      // 1. Read the Record Header Size
      var (headerSize, headerSizeLen) = VarintHelper.ReadVarint(databaseFile);

      // 2. Read all the Serial Type Varints in the header
      int bytesToRead = (int)headerSize - headerSizeLen;
      byte[] headerBuffer = new byte[bytesToRead];
      databaseFile.ReadExactly(headerBuffer);

      List<int> columnLengths = [];
      int offset = 0;
      while (offset < headerBuffer.Length)
      {
        ulong serialType = VarintHelper.ReadVarint(headerBuffer, ref offset);
        columnLengths.Add(GetSerialTypeLength(serialType));
      }

      // 3. Read the columns from the body using the lengths
      int i = 0;
      List<string> output = [.. new string[parsedInput.Selected.Count]];

      foreach (var column in table.Columns)
      {
        byte[] dataBytes = new byte[columnLengths[i++]];
        databaseFile.ReadExactly(dataBytes);

        var insertIndex = parsedInput.Selected.IndexOf(column.Name);

        if (insertIndex == -1)
          continue;

        output[insertIndex] = Encoding.UTF8.GetString(dataBytes);
      }

      Console.WriteLine(string.Join(" | ", output));
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

  private static long ReadBigEndianInteger(ReadOnlySpan<byte> buffer)
  {
    long value = 0;
    foreach (byte b in buffer)
      value = (value << 8) | b;
    return value;
  }
}