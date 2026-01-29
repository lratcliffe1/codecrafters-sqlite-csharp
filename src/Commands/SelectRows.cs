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
    BTreePageHeader schemaHeader = HeaderHelper.ReadPageHeader(databaseFile, 1, databaseHeader.PageSize);
    ParsedSelectQuery query = SqlParseHelper.ParseSelect(command);

    List<int> schemaCellPointers = CellPointerHelper.ReadCellPointers(
      databaseFile,
      schemaHeader.PageType,
      1,
      databaseHeader.PageSize,
      schemaHeader.CellCount
    );
    List<Record> schemaRecords = RecordHelper.ReadSchemaRecords(databaseFile, schemaCellPointers);

    Record tableRecord = schemaRecords.First(x => x.Name == query.TableName && x.Type != "index");
    TableData table = RecordHelper.ParseTableSchema(tableRecord);

    HashSet<long>? indexedRowIds = TryGetIndexedRowIds(databaseFile, databaseHeader, schemaRecords, query);

    if (indexedRowIds != null)
    {
      foreach (var rowId in indexedRowIds)
        PrintRowById(databaseFile, databaseHeader, table, query, table.RootPage, rowId);

      return;
    }

    BTreePageHeader tableHeader = HeaderHelper.ReadPageHeader(databaseFile, table.RootPage, databaseHeader.PageSize);
    ScanTableTree(databaseFile, databaseHeader, tableHeader, table, query, table.RootPage, null);
  }

  private static HashSet<long>? TryGetIndexedRowIds(FileStream databaseFile, DatabaseHeader databaseHeader, List<Record> schemaRecords, ParsedSelectQuery query)
  {
    if (string.IsNullOrEmpty(query.WhereColumn) || string.IsNullOrEmpty(query.WhereValue))
      return null;

    foreach (var schemaRecord in schemaRecords)
    {
      if (schemaRecord.Type != "index")
        continue;

      ParsedIndexDefinition indexDefinition = SqlParseHelper.ParseIndexSql(schemaRecord.Sql);

      if (!string.Equals(indexDefinition.OnTable, query.TableName, StringComparison.OrdinalIgnoreCase))
        continue;

      if (!string.Equals(indexDefinition.OnColumn, query.WhereColumn, StringComparison.OrdinalIgnoreCase))
        continue;

      BTreePageHeader indexHeader = HeaderHelper.ReadPageHeader(databaseFile, schemaRecord.RootPage, databaseHeader.PageSize);
      HashSet<long> rowIds = [];
      CollectMatchingRowIds(databaseFile, databaseHeader, indexHeader, query.WhereValue, schemaRecord.RootPage, rowIds);
      return rowIds;
    }

    return null;
  }

  private static void CollectMatchingRowIds(FileStream databaseFile, DatabaseHeader databaseHeader, BTreePageHeader indexHeader, string whereValue, int pageNumber, HashSet<long> rowIds)
  {
    List<int> cellPointers = CellPointerHelper.ReadCellPointers(databaseFile, indexHeader.PageType, pageNumber, databaseHeader.PageSize, indexHeader.CellCount);

    long pageStart = (pageNumber - 1) * databaseHeader.PageSize;

    if (indexHeader.PageType == LeafIndexPageType)
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

    if (indexHeader.PageType != InteriorIndexPageType)
      return;

    foreach (var childPage in EnumerateIndexChildPages(databaseFile, pageStart, cellPointers, indexHeader.RightMostPointer, whereValue))
    {
      BTreePageHeader childHeader = HeaderHelper.ReadPageHeader(databaseFile, childPage, databaseHeader.PageSize);
      CollectMatchingRowIds(databaseFile, databaseHeader, childHeader, whereValue, childPage, rowIds);
    }
  }

  private static IEnumerable<int> EnumerateIndexChildPages(FileStream databaseFile, long pageStart, IEnumerable<int> cellPointers, uint? rightMostPointer, string whereValue)
  {
    foreach (var pointer in cellPointers)
    {
      databaseFile.Seek(pageStart + pointer, SeekOrigin.Begin);
      byte[] childBuffer = new byte[4];
      databaseFile.ReadExactly(childBuffer);
      int childPage = (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(childBuffer);

      string key = RecordHelper.ReadIndexKeyFromCell(databaseFile);
      int compare = CompareIndexKeys(whereValue, key);

      if (compare <= 0)
      {
        yield return childPage;
        yield break;
      }
    }

    if (rightMostPointer.HasValue)
      yield return (int)rightMostPointer.Value;
  }

  private static int CompareIndexKeys(string left, string right)
  {
    if (long.TryParse(left, out long leftLong) && long.TryParse(right, out long rightLong))
      return leftLong.CompareTo(rightLong);

    if (double.TryParse(left, out double leftDouble) && double.TryParse(right, out double rightDouble))
      return leftDouble.CompareTo(rightDouble);

    return string.CompareOrdinal(left, right);
  }

  private static void ScanTableTree(FileStream databaseFile, DatabaseHeader databaseHeader, BTreePageHeader tableHeader, TableData table, ParsedSelectQuery query, int pageNumber, HashSet<long>? allowedRowIds)
  {
    List<int> cellPointers = CellPointerHelper.ReadCellPointers(databaseFile, tableHeader.PageType, pageNumber, databaseHeader.PageSize, tableHeader.CellCount);

    if (tableHeader.PageType == LeafTablePageType)
    {
      RecordHelper.PrintLeafRows(databaseFile, databaseHeader, table, cellPointers, query, pageNumber, allowedRowIds);
      return;
    }

    long pageStart = (pageNumber - 1) * databaseHeader.PageSize;

    foreach (var childPage in EnumerateChildPages(databaseFile, pageStart, cellPointers, tableHeader.RightMostPointer))
    {
      BTreePageHeader childHeader = HeaderHelper.ReadPageHeader(databaseFile, childPage, databaseHeader.PageSize);
      ScanTableTree(databaseFile, databaseHeader, childHeader, table, query, childPage, allowedRowIds);
    }
  }

  private static IEnumerable<int> EnumerateChildPages(FileStream databaseFile, long pageStart, IEnumerable<int> cellPointers, uint? rightMostPointer)
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

  private static void PrintRowById(FileStream databaseFile, DatabaseHeader databaseHeader, TableData table, ParsedSelectQuery query, int pageNumber, long rowId)
  {
    BTreePageHeader header = HeaderHelper.ReadPageHeader(databaseFile, pageNumber, databaseHeader.PageSize);

    List<int> cellPointers = CellPointerHelper.ReadCellPointers(databaseFile, header.PageType, pageNumber, databaseHeader.PageSize, header.CellCount);

    if (header.PageType == LeafTablePageType)
    {
      RecordHelper.PrintLeafRows(databaseFile, databaseHeader, table, cellPointers, query, pageNumber, new HashSet<long> { rowId });
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
        PrintRowById(databaseFile, databaseHeader, table, query, childPage, rowId);
        return;
      }
    }

    if (header.RightMostPointer.HasValue)
      PrintRowById(databaseFile, databaseHeader, table, query, (int)header.RightMostPointer.Value, rowId);
  }
}
