using codecrafters_sqlite.src.Classes;
using codecrafters_sqlite.src.Helpers;

namespace codecrafters_sqlite.src.Commands;

public class SelectRows
{
  private const int LeafTablePageType = 13;
  private const int LeafIndexPageType = 10;
  private const int InteriorTablePageType = 5;
  private const int InteriorIndexPageType = 2;

  public static void Process(FileStream databaseFile, string command)
  {
    DatabaseHeader databaseHeader = HeaderHelper.ReadDatabaseHeader(databaseFile);
    BTreePageHeader bTreeHeader = HeaderHelper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);

    ParsedInput parsedInput = ParceInputSqlHelper.ParseInput(command);

    List<int> cellPointerArray = CellPointerHelper.GetCellPointerArray(databaseFile, bTreeHeader.PageType, 1, databaseHeader.PageSize, bTreeHeader.CellCount);
    List<Record> tables = RecordHelper.GetRecordData(databaseFile, cellPointerArray);

    Record record = tables.First(x => x.Name == parsedInput.TableName && x.Type != "index");
    TableData table = RecordHelper.ParceTableData(record);

    BTreePageHeader bTreePageHeader = HeaderHelper.ReadPageHeader(databaseFile, table.RootPage, databaseHeader.PageSize);

    HashSet<long>? indexedRowIds = GetRowIdsFromIndex(databaseFile, databaseHeader, tables, parsedInput);

    if (indexedRowIds != null)
    {
      foreach (var rowId in indexedRowIds)
        FindAndPrintRowById(databaseFile, databaseHeader, table, parsedInput, table.RootPage, rowId);

      return;
    }

    RecursivelyCheckPointers(databaseFile, databaseHeader, bTreePageHeader, table, parsedInput, table.RootPage, null);
  }

  private static HashSet<long>? GetRowIdsFromIndex(FileStream databaseFile, DatabaseHeader databaseHeader, List<Record> tables, ParsedInput parsedInput)
  {
    if (string.IsNullOrEmpty(parsedInput.WhereColumn) || string.IsNullOrEmpty(parsedInput.WhereValue))
      return null;

    foreach (var t in tables)
    {
      if (t.Type != "index")
        continue;

      ParsedIndexSQL parsedIndexSQL = ParceInputSqlHelper.ParseIndexSQL(t.SQL);

      if (!string.Equals(parsedIndexSQL.OnTable, parsedInput.TableName, StringComparison.OrdinalIgnoreCase))
        continue;

      if (!string.Equals(parsedIndexSQL.OnColumn, parsedInput.WhereColumn, StringComparison.OrdinalIgnoreCase))
        continue;

      BTreePageHeader bTreeIndexHeader = HeaderHelper.ReadPageHeader(databaseFile, t.RootPage, databaseHeader.PageSize);
      HashSet<long> rowIds = [];
      CollectRowIdsFromIndex(databaseFile, databaseHeader, bTreeIndexHeader, parsedInput.WhereValue, t.RootPage, rowIds);
      return rowIds;
    }

    return null;
  }

  private static void CollectRowIdsFromIndex(FileStream databaseFile, DatabaseHeader databaseHeader, BTreePageHeader bTreePageHeader, string whereValue, int pageNumber, HashSet<long> rowIds)
  {
    List<int> cellPointers = CellPointerHelper.GetCellPointerArray(databaseFile, bTreePageHeader.PageType, pageNumber, databaseHeader.PageSize, bTreePageHeader.CellCount);

    long pageStart = (pageNumber - 1) * databaseHeader.PageSize;

    if (bTreePageHeader.PageType == LeafIndexPageType)
    {

      foreach (var pointer in cellPointers)
      {
        databaseFile.Seek(pageStart + pointer, SeekOrigin.Begin);
        var (key, rowId) = RecordHelper.ReadIndexLeafEntry(databaseFile);
        if (key == whereValue)
          rowIds.Add(rowId);
      }

      return;
    }

    if (bTreePageHeader.PageType != InteriorIndexPageType)
      return;

    foreach (var childPage in GetIndexChildPages(databaseFile, pageStart, cellPointers, bTreePageHeader.RightMostPointer, whereValue))
    {
      BTreePageHeader childHeader = HeaderHelper.ReadPageHeader(databaseFile, childPage, databaseHeader.PageSize);
      CollectRowIdsFromIndex(databaseFile, databaseHeader, childHeader, whereValue, childPage, rowIds);
    }
  }

  private static IEnumerable<int> GetIndexChildPages(FileStream databaseFile, long pageStart, IEnumerable<int> cellPointers, uint? rightMostPointer, string whereValue)
  {
    foreach (var pointer in cellPointers)
    {
      databaseFile.Seek(pageStart + pointer, SeekOrigin.Begin);
      byte[] childBuffer = new byte[4];
      databaseFile.ReadExactly(childBuffer);
      int childPage = (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(childBuffer);

      string key = RecordHelper.ReadIndexKeyFromCell(databaseFile);
      int compare = CompareIndexValues(whereValue, key);

      if (compare <= 0)
      {
        yield return childPage;
        yield break;
      }
    }

    if (rightMostPointer.HasValue)
      yield return (int)rightMostPointer.Value;
  }

  private static int CompareIndexValues(string left, string right)
  {
    if (long.TryParse(left, out long leftLong) && long.TryParse(right, out long rightLong))
      return leftLong.CompareTo(rightLong);

    if (double.TryParse(left, out double leftDouble) && double.TryParse(right, out double rightDouble))
      return leftDouble.CompareTo(rightDouble);

    return string.CompareOrdinal(left, right);
  }

  private static void RecursivelyCheckPointers(FileStream databaseFile, DatabaseHeader databaseHeader, BTreePageHeader bTreePageHeader, TableData table, ParsedInput parsedInput, int pageNumber, HashSet<long>? allowedRowIds)
  {
    List<int> cellPointers = CellPointerHelper.GetCellPointerArray(databaseFile, bTreePageHeader.PageType, pageNumber, databaseHeader.PageSize, bTreePageHeader.CellCount);

    if (bTreePageHeader.PageType == LeafTablePageType)
    {
      RecordHelper.PrintRecordValues(databaseFile, databaseHeader, table, cellPointers, parsedInput, pageNumber, allowedRowIds);
      return;
    }

    long pageStart = (pageNumber - 1) * databaseHeader.PageSize;

    foreach (var childPage in GetChildPages(databaseFile, pageStart, cellPointers, bTreePageHeader.RightMostPointer))
    {
      BTreePageHeader childHeader = HeaderHelper.ReadPageHeader(databaseFile, childPage, databaseHeader.PageSize);
      RecursivelyCheckPointers(databaseFile, databaseHeader, childHeader, table, parsedInput, childPage, allowedRowIds);
    }
  }

  private static IEnumerable<int> GetChildPages(FileStream databaseFile, long pageStart, IEnumerable<int> cellPointers, uint? rightMostPointer)
  {
    foreach (var pointer in cellPointers)
    {
      yield return ReadChildPageNumber(databaseFile, pageStart, pointer);
    }

    if (rightMostPointer.HasValue)
      yield return (int)rightMostPointer.Value;
  }

  private static int ReadChildPageNumber(FileStream databaseFile, long pageStart, int cellPointer)
  {
    databaseFile.Seek(pageStart + cellPointer, SeekOrigin.Begin);
    byte[] buffer = new byte[4];
    databaseFile.ReadExactly(buffer);
    return (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(buffer);
  }

  private static void FindAndPrintRowById(FileStream databaseFile, DatabaseHeader databaseHeader, TableData table, ParsedInput parsedInput, int pageNumber, long rowId)
  {
    BTreePageHeader header = HeaderHelper.ReadPageHeader(databaseFile, pageNumber, databaseHeader.PageSize);

    List<int> cellPointers = CellPointerHelper.GetCellPointerArray(databaseFile, header.PageType, pageNumber, databaseHeader.PageSize, header.CellCount);

    if (header.PageType == LeafTablePageType)
    {
      RecordHelper.PrintRecordValues(databaseFile, databaseHeader, table, cellPointers, parsedInput, pageNumber, new HashSet<long> { rowId });
      return;
    }

    if (header.PageType != InteriorTablePageType)
      return;

    long pageStart = (pageNumber - 1) * databaseHeader.PageSize;

    foreach (var pointer in cellPointers)
    {
      databaseFile.Seek(pageStart + pointer, SeekOrigin.Begin);
      byte[] childBuffer = new byte[4];
      databaseFile.ReadExactly(childBuffer);
      int childPage = (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(childBuffer);

      var (key, _) = VarintHelper.ReadVarint(databaseFile);
      if (rowId <= (long)key)
      {
        FindAndPrintRowById(databaseFile, databaseHeader, table, parsedInput, childPage, rowId);
        return;
      }
    }

    if (header.RightMostPointer.HasValue)
      FindAndPrintRowById(databaseFile, databaseHeader, table, parsedInput, (int)header.RightMostPointer.Value, rowId);
  }
}
