
using System.Text;
using codecrafters_sqlite.src.Classes;

namespace codecrafters_sqlite.src.Helpers;

public static class RecordHelper
{
  public static void PrintLeafRows(
    Stream databaseFile,
    DatabaseHeader databaseHeader,
    TableData table,
    List<int> cellPointerArray,
    ParsedSelectQuery parsedQuery,
    int pageNumber,
    HashSet<long>? allowedRowIds = null)
  {
    // Compute the byte offset of the page start within the file.
    // Every page has a fixed size, so we can jump directly to it.
    var pageStart = PageHelper.GetPageStart(databaseHeader.PageSize, pageNumber);

    // Each cell pointer points to one record cell in this leaf page.
    // We iterate in pointer order, which is how SQLite stores them.
    foreach (ushort pointer in cellPointerArray)
    {
      // Seek to the start of the cell so we can read its payload.
      databaseFile.Seek(pageStart + pointer, SeekOrigin.Begin);

      // Read payload size varint (unused here, but advances the stream correctly).
      // We still need to consume it because the next bytes are the rowid.
      var (_, _) = VarintHelper.ReadVarint(databaseFile);
      // Read rowid varint (table b-tree key).
      // This is the primary key for rowid tables.
      var (rowId, _) = VarintHelper.ReadVarint(databaseFile);

      // If we have a rowid filter, skip this row early.
      // This saves us from parsing the full record when we already know
      // we do not need it.
      if (allowedRowIds != null && !allowedRowIds.Contains((long)rowId))
        continue;

      // Read the record header size varint.
      // The header tells us how each column is encoded.
      var (headerSize, headerSizeLen) = VarintHelper.ReadVarint(databaseFile);

      // Read all serial type varints from the record header.
      // Each serial type corresponds to one column in the table schema.
      List<ulong> serialTypes = ReadSerialTypes(databaseFile, headerSize, headerSizeLen);

      // Prepare output buffers for selected columns and WHERE evaluation.
      // selectedValues is in SELECT order; rowValues is for WHERE checks.
      int i = 0;
      List<string> selectedValues = [.. new string[parsedQuery.Selected.Count]];
      Dictionary<string, string> rowValues = [];

      // Read each column in schema order so we stay aligned with serialTypes.
      foreach (var column in table.Columns)
      {
        // Get this column's serial type and payload length.
        // Length tells us how many bytes to read for this column.
        ulong serialType = serialTypes[i++];
        int length = GetSerialTypeLength(serialType);
        // INTEGER PRIMARY KEY can alias rowid and not appear in payload.
        // In that case, SQLite stores it as length 0 in the payload.
        bool isRowIdAlias = length == 0
          && column.IsPrimaryKey
          && string.Equals(column.Type, "integer", StringComparison.OrdinalIgnoreCase);

        if (isRowIdAlias)
        {
          // Use rowid for the aliased column value.
          string rowIdValue = rowId.ToString();
          rowValues.Add(column.Name, rowIdValue);

          // Store value if the column is in SELECT.
          // We place it in the correct position in the output.
          var insertIndex = parsedQuery.Selected.IndexOf(column.Name);
          if (insertIndex != -1)
            selectedValues[insertIndex] = rowIdValue;

          // Skip reading a payload value because it does not exist.
          continue;
        }

        // Read the column value from the payload using the serial type.
        string value = ReadSerialValue(databaseFile, serialType, length);

        // Store the value for WHERE predicate evaluation.
        rowValues.Add(column.Name, value);

        // Store the value if this column was selected.
        // If it is not in SELECT, we only keep it for WHERE evaluation.
        var selectedIndex = parsedQuery.Selected.IndexOf(column.Name);
        if (selectedIndex == -1)
          continue;

        selectedValues[selectedIndex] = value;
      }

      // Apply the WHERE predicate to the row values.
      // If it returns false, this row is filtered out.
      if (!parsedQuery.WherePredicate(rowValues))
        continue;

      // Emit the selected columns in SELECT order.
      Console.WriteLine(string.Join("|", selectedValues));
    }
  }

  public static List<Record> ReadSchemaRecords(Stream file, List<int> cellPointers)
  {
    // Each pointer corresponds to one sqlite_schema row.
    // We read each one and return them as Record objects.
    return cellPointers.Select(x => ReadSchemaRecord(file, x)).ToList();
  }

  public static Record ReadSchemaRecord(Stream file, int cellOffset)
  {
    // Seek to the start of the schema cell.
    file.Seek(cellOffset, SeekOrigin.Begin);

    // Read and skip payload size (not needed here).
    var (_, _) = VarintHelper.ReadVarint(file);
    // Read and skip rowid (sqlite_schema rowid is not needed).
    var (_, _) = VarintHelper.ReadVarint(file); // Table B-Tree key

    // Read header size for the record payload.
    var (headerSize, headerSizeLen) = VarintHelper.ReadVarint(file);
    // The header contains the serial types for each column.
    int bytesToReadForTypes = (int)headerSize - headerSizeLen;
    byte[] typeBuffer = new byte[bytesToReadForTypes];
    file.ReadExactly(typeBuffer);

    // Decode each serial type varint from the header.
    List<ulong> serialTypeCodes = [];
    int offset = 0;
    while (offset < typeBuffer.Length)
    {
      serialTypeCodes.Add(VarintHelper.ReadVarint(typeBuffer, ref offset));
    }

    // sqlite_schema has 5 columns we care about.
    string[] columnValues = new string[5];

    // Read the payload values based on the serial types.
    for (int i = 0; i < serialTypeCodes.Count; i++)
    {
      int len = GetSerialTypeLength(serialTypeCodes[i]);
      byte[] data = new byte[len];
      if (len > 0) file.ReadExactly(data);

      // Column 3 is rootpage which is stored as a big-endian integer.
      if (i == 3) // rootpage column
        columnValues[i] = ReadBigEndianUnsignedInteger(data).ToString();
      // Other columns are stored as text.
      else if (i < 5)
        columnValues[i] = Encoding.UTF8.GetString(data);
    }

    // Build a Record from the parsed schema row.
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
    // Extract the column list from the CREATE TABLE SQL.
    List<string> sqlData = record.Sql
      .Split("(").Last()
      .Split(")").First()
      .Replace("\n", "")
      .Split(',')
      .ToList();

    // Build a Column list from the SQL pieces.
    List<Column> columns = [];

    // Each entry is "name type [primary key]".
    foreach (var row in sqlData)
    {
      List<string> rowData = row.Trim().Split(" ").ToList();

      // Create a Column with name, type, and primary key flag.
      columns.Add(new Column
      {
        Name = rowData[0].Trim(),
        Type = rowData[1].Trim(),
        IsPrimaryKey = rowData.Count > 3 && rowData[2] == "primary" && rowData[3] == "key",
      });
    }

    // Return a TableData object with metadata and columns.
    return new TableData()
    {
      Type = record.Type,
      Name = record.Name,
      TableName = record.TableName,
      RootPage = record.RootPage,
      Columns = columns,
    };
  }

  public static (string Key, long RowId) ReadIndexLeafEntry(Stream file)
  {
    // Index leaf cell starts with a payload length varint.
    // We read it to move the stream to the record payload.
    var (_, _) = VarintHelper.ReadVarint(file);
    // Payload is a record containing the index key and rowid.
    // SQLite stores index entries as a tiny record.
    List<string> values = ReadPayloadValues(file);

    // Expect at least [key, rowid].
    // If not, the index cell is malformed for our expectations.
    if (values.Count < 2)
      throw new InvalidDataException("Index record did not contain key and rowid.");

    // First value is the indexed key.
    string key = values[0];
    // Last value is the rowid (table key).
    // We parse it to a numeric type for comparisons.
    if (!long.TryParse(values[^1], out long rowId))
      throw new InvalidDataException("Index rowid could not be parsed as integer.");

    // Return the key/rowid pair to the caller.
    return (key, rowId);
  }

  public static string ReadIndexKeyFromCell(Stream file)
  {
    // Index interior cells also start with a payload length varint.
    var (_, _) = VarintHelper.ReadVarint(file);
    // The payload contains the index key; we read the record values.
    List<string> values = ReadPayloadValues(file);

    // If there is no key, the cell is malformed.
    if (values.Count == 0)
      throw new InvalidDataException("Index record did not contain a key.");

    // The first value is the key.
    return values[0];
  }

  private static List<string> ReadPayloadValues(Stream file)
  {
    // Read the record header size.
    var (headerSize, headerSizeLen) = VarintHelper.ReadVarint(file);
    // Decode the serial types from the header.
    List<ulong> serialTypes = ReadSerialTypes(file, headerSize, headerSizeLen);

    // Read each value from the payload using its serial type.
    List<string> values = [];
    foreach (var serialType in serialTypes)
    {
      int length = GetSerialTypeLength(serialType);
      values.Add(ReadSerialValue(file, serialType, length));
    }

    // Return the decoded values as strings.
    return values;
  }

  private static List<ulong> ReadSerialTypes(Stream file, ulong headerSize, int headerSizeLen)
  {
    // The header includes the size of itself, so subtract that.
    int bytesToRead = (int)headerSize - headerSizeLen;
    byte[] headerBuffer = new byte[bytesToRead];
    file.ReadExactly(headerBuffer);

    // Parse each varint serial type from the header buffer.
    List<ulong> serialTypes = [];
    int offset = 0;
    while (offset < headerBuffer.Length)
    {
      serialTypes.Add(VarintHelper.ReadVarint(headerBuffer, ref offset));
    }

    // Return the list of serial types in column order.
    return serialTypes;
  }

  private static int GetSerialTypeLength(ulong serialType)
  {
    // Convert SQLite serial type codes to payload byte lengths.
    // This tells us how many bytes to read for each column value.
    return serialType switch
    {
      // 0 = NULL, no bytes in payload.
      0 => 0,
      // 1..6 are integers with fixed widths.
      1 => 1,
      2 => 2,
      3 => 3,
      4 => 4,
      5 => 6,
      6 => 8,
      // 7 = 8-byte floating point.
      7 => 8,
      // 8 and 9 are constants 0 and 1 with no payload bytes.
      8 => 0,
      9 => 0,
      // 12+ encodes blob/text lengths (even=blob, odd=text).
      // SQLite packs the length into the serial type itself.
      _ => serialType >= 12
        ? (int)((serialType - (serialType % 2 == 0 ? 12u : 13u)) / 2)
        : 0
    };
  }

  private static string ReadSerialValue(Stream file, ulong serialType, int length)
  {
    // Serial type 0 represents NULL; we return empty string.
    if (serialType == 0)
      return string.Empty;

    // Serial type 8 is integer constant 0.
    if (serialType == 8)
      return "0";

    // Serial type 9 is integer constant 1.
    if (serialType == 9)
      return "1";

    // Serial type 7 is an 8-byte floating point number.
    if (serialType == 7)
    {
      byte[] data = new byte[8];
      file.ReadExactly(data);
      if (BitConverter.IsLittleEndian)
        Array.Reverse(data);
      double value = BitConverter.ToDouble(data, 0);
      return value.ToString();
    }

    // Serial types 1..6 are signed integers with different widths.
    if (serialType is >= 1 and <= 6)
    {
      byte[] data = new byte[length];
      file.ReadExactly(data);
      long value = ReadBigEndianSignedInteger(data);
      return value.ToString();
    }

    // For text/blob types, read raw bytes of the given length.
    byte[] dataBytes = new byte[length];
    if (length > 0)
      file.ReadExactly(dataBytes);

    // Even serial types >= 12 represent blobs (binary); return hex.
    if (serialType >= 12 && serialType % 2 == 0)
      return Convert.ToHexString(dataBytes);

    // Odd serial types >= 13 represent text; decode as UTF-8.
    return Encoding.UTF8.GetString(dataBytes);
  }

  private static long ReadBigEndianUnsignedInteger(ReadOnlySpan<byte> buffer)
  {
    // Build a positive integer from big-endian bytes.
    long value = 0;
    foreach (byte b in buffer)
      value = (value << 8) | b;
    return value;
  }

  private static long ReadBigEndianSignedInteger(ReadOnlySpan<byte> buffer)
  {
    // Read the bytes as an unsigned integer first.
    long value = ReadBigEndianUnsignedInteger(buffer);
    // Determine how many bits are present in the value.
    int bitCount = buffer.Length * 8;
    // The top bit indicates the sign for two's complement numbers.
    long signBit = 1L << (bitCount - 1);
    // If the sign bit is set, convert to a negative value.
    if ((value & signBit) != 0)
      value -= 1L << bitCount;
    return value;
  }
}