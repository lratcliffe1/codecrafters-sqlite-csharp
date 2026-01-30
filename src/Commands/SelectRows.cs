using codecrafters_sqlite.src.Classes;
using codecrafters_sqlite.src.Helpers;

namespace codecrafters_sqlite.src.Commands;

public class SelectRows
{
  public static void Process(Stream databaseFile, string command)
  {
    // Read the database header to get global settings like page size.
    DatabaseHeader databaseHeader = HeaderHelper.ReadDatabaseHeader(databaseFile);
    // Read the schema page header (page 1) so we can find schema records.
    BTreePageHeader schemaHeader = HeaderHelper.ReadPageHeader(databaseFile, SqliteConstants.SchemaPageNumber, databaseHeader.PageSize);
    // Parse the SQL text into a structured object we can work with.
    ParsedSelectQuery query = SqlParseHelper.ParseSelect(command);

    // Read the cell pointers for the schema page.
    List<int> schemaCellPointers = CellPointerHelper.ReadCellPointers(databaseFile, schemaHeader.PageType, SqliteConstants.SchemaPageNumber, databaseHeader.PageSize, schemaHeader.CellCount);
    // Read all schema records from those pointers (tables, indexes, etc.).
    List<Record> schemaRecords = RecordHelper.ReadSchemaRecords(databaseFile, schemaCellPointers);

    // Find the schema record for the target table (ignore index records).
    Record tableRecord = schemaRecords.First(x => x.Name == query.TableName && x.Type != "index");
    // Parse the CREATE TABLE SQL into a list of columns.
    TableData table = RecordHelper.ParseTableSchema(tableRecord);

    // Try to resolve the WHERE clause using a matching index.
    HashSet<long>? indexedRowIds = TryGetIndexedRowIds(databaseFile, databaseHeader, schemaRecords, query);

    // If we have rowids from the index, fetch rows by rowid only.
    if (indexedRowIds != null)
    {
      foreach (var rowId in indexedRowIds)
        PrintRowById(databaseFile, databaseHeader, table, query, table.RootPage, rowId);

      return;
    }

    // Otherwise do a full table scan.
    BTreePageHeader tableHeader = HeaderHelper.ReadPageHeader(databaseFile, table.RootPage, databaseHeader.PageSize);
    ScanTableTree(databaseFile, databaseHeader, tableHeader, table, query, table.RootPage, null);
  }

  private static HashSet<long>? TryGetIndexedRowIds(Stream databaseFile, DatabaseHeader databaseHeader, List<Record> schemaRecords, ParsedSelectQuery query)
  {
    // If there is no WHERE clause, we cannot use an index.
    if (string.IsNullOrEmpty(query.WhereColumn) || string.IsNullOrEmpty(query.WhereValue))
      return null;

    // Search the schema for an index that matches the table and WHERE column.
    foreach (var schemaRecord in schemaRecords)
    {
      // Skip non-index records.
      if (schemaRecord.Type != "index")
        continue;

      // Parse the CREATE INDEX SQL to get table/column info.
      ParsedIndexDefinition indexDefinition = SqlParseHelper.ParseIndexSql(schemaRecord.Sql);

      // Only use indexes that belong to our target table.
      if (!string.Equals(indexDefinition.OnTable, query.TableName, StringComparison.OrdinalIgnoreCase))
        continue;

      // Only use indexes on the specific WHERE column.
      if (!string.Equals(indexDefinition.OnColumn, query.WhereColumn, StringComparison.OrdinalIgnoreCase))
        continue;

      // Read the root page of the index b-tree.
      BTreePageHeader indexHeader = HeaderHelper.ReadPageHeader(databaseFile, schemaRecord.RootPage, databaseHeader.PageSize);
      // Collect rowids by walking the index.
      HashSet<long> rowIds = [];
      CollectMatchingRowIds(databaseFile, databaseHeader, indexHeader, query.WhereValue, schemaRecord.RootPage, rowIds);
      return rowIds;
    }

    // No usable index was found.
    return null;
  }

  private static void CollectMatchingRowIds(Stream databaseFile, DatabaseHeader databaseHeader, BTreePageHeader indexHeader, string whereValue, int pageNumber, HashSet<long> rowIds)
  {
    // Read the cell pointer array for this index page.
    // These pointers tell us where each index cell starts.
    List<int> cellPointers = CellPointerHelper.ReadCellPointers(databaseFile, indexHeader.PageType, pageNumber, databaseHeader.PageSize, indexHeader.CellCount);

    // Compute the start offset for this page so we can seek into it.
    long pageStart = PageHelper.GetPageStart(databaseHeader.PageSize, pageNumber);

    // Leaf index pages contain (key,rowid) records.
    // That is where we actually collect rowids.
    if (indexHeader.PageType == SqliteConstants.LeafIndexPageType)
    {
      // Scan each leaf cell and collect rowids matching the key.
      foreach (var pointer in cellPointers)
      {
        databaseFile.Seek(pageStart + pointer, SeekOrigin.Begin);
        var (key, rowId) = RecordHelper.ReadIndexLeafEntry(databaseFile);
        if (key == whereValue)
          rowIds.Add(rowId);
      }

      return;
    }

    // Only interior index pages are navigable for index descent.
    // If this is some unexpected page type, stop here.
    if (indexHeader.PageType != SqliteConstants.InteriorIndexPageType)
      return;

    // Follow the correct child page based on key comparison.
    // We only descend into the branch that can contain whereValue.
    foreach (var childPage in EnumerateIndexChildPages(databaseFile, pageStart, cellPointers, indexHeader.RightMostPointer, whereValue))
    {
      BTreePageHeader childHeader = HeaderHelper.ReadPageHeader(databaseFile, childPage, databaseHeader.PageSize);
      CollectMatchingRowIds(databaseFile, databaseHeader, childHeader, whereValue, childPage, rowIds);
    }
  }

  private static IEnumerable<int> EnumerateIndexChildPages(Stream databaseFile, long pageStart, IEnumerable<int> cellPointers, uint? rightMostPointer, string whereValue)
  {
    // Each interior index cell: [child page][key].
    // We choose the first child whose key is >= target.
    foreach (var pointer in cellPointers)
    {
      // Read the child page number (first 4 bytes of the cell).
      int childPage = ReadChildPageNumber(databaseFile, pageStart, pointer);

      // Read the key that separates this child from the next.
      // The key is stored as a record payload.
      string key = RecordHelper.ReadIndexKeyFromCell(databaseFile);
      // Compare target WHERE value to this key.
      // If target is <= key, the row must be in this child.
      int compare = CompareIndexKeys(whereValue, key);

      // If target is less than or equal to this key, descend into this child.
      if (compare <= 0)
      {
        yield return childPage;
        yield break;
      }
    }

    // If target is greater than all keys, follow the right-most pointer.
    if (rightMostPointer.HasValue)
      yield return (int)rightMostPointer.Value;
  }

  private static int CompareIndexKeys(string left, string right)
  {
    // Try integer comparison first for numeric keys.
    if (long.TryParse(left, out long leftLong) && long.TryParse(right, out long rightLong))
      return leftLong.CompareTo(rightLong);

    // If integers fail, try floating point.
    if (double.TryParse(left, out double leftDouble) && double.TryParse(right, out double rightDouble))
      return leftDouble.CompareTo(rightDouble);

    // Fall back to ordinal string comparison.
    return string.CompareOrdinal(left, right);
  }

  private static void ScanTableTree(Stream databaseFile, DatabaseHeader databaseHeader, BTreePageHeader tableHeader, TableData table, ParsedSelectQuery query, int pageNumber, HashSet<long>? allowedRowIds)
  {
    // Read the cell pointers for this table page.
    List<int> cellPointers = CellPointerHelper.ReadCellPointers(databaseFile, tableHeader.PageType, pageNumber, databaseHeader.PageSize, tableHeader.CellCount);

    // If this is a leaf page, read and print the rows.
    if (tableHeader.PageType == SqliteConstants.LeafTablePageType)
    {
      RecordHelper.PrintLeafRows(databaseFile, databaseHeader, table, cellPointers, query, pageNumber, allowedRowIds);
      return;
    }

    // Compute the page start for reading child pointers.
    long pageStart = PageHelper.GetPageStart(databaseHeader.PageSize, pageNumber);

    // Recursively scan each child page.
    foreach (var childPage in EnumerateChildPages(databaseFile, pageStart, cellPointers, tableHeader.RightMostPointer))
    {
      BTreePageHeader childHeader = HeaderHelper.ReadPageHeader(databaseFile, childPage, databaseHeader.PageSize);
      ScanTableTree(databaseFile, databaseHeader, childHeader, table, query, childPage, allowedRowIds);
    }
  }

  private static IEnumerable<int> EnumerateChildPages(Stream databaseFile, long pageStart, IEnumerable<int> cellPointers, uint? rightMostPointer)
  {
    // Each interior cell stores a child page pointer.
    foreach (var pointer in cellPointers)
    {
      yield return ReadChildPageNumber(databaseFile, pageStart, pointer);
    }

    // The right-most pointer is stored separately.
    if (rightMostPointer.HasValue)
      yield return (int)rightMostPointer.Value;
  }

  private static int ReadChildPageNumber(Stream databaseFile, long pageStart, int cellPointer)
  {
    // The first 4 bytes of a cell are the child page number.
    databaseFile.Seek(pageStart + cellPointer, SeekOrigin.Begin);
    byte[] buffer = new byte[4];
    databaseFile.ReadExactly(buffer);
    return (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(buffer);
  }

  private static void PrintRowById(Stream databaseFile, DatabaseHeader databaseHeader, TableData table, ParsedSelectQuery query, int pageNumber, long rowId)
  {
    // Read the current table b-tree page header.
    // This tells us whether the page is leaf or interior.
    BTreePageHeader header = HeaderHelper.ReadPageHeader(databaseFile, pageNumber, databaseHeader.PageSize);

    // Read the cell pointers for this table page.
    List<int> cellPointers = CellPointerHelper.ReadCellPointers(databaseFile, header.PageType, pageNumber, databaseHeader.PageSize, header.CellCount);

    // Leaf table pages contain row records; filter by rowid.
    // We use a one-item allowedRowIds set to print only this row.
    if (header.PageType == SqliteConstants.LeafTablePageType)
    {
      RecordHelper.PrintLeafRows(databaseFile, databaseHeader, table, cellPointers, query, pageNumber, new HashSet<long> { rowId });
      return;
    }

    // If we are not on an interior table page, we cannot descend.
    // This guards against unexpected page types.
    if (header.PageType != SqliteConstants.InteriorTablePageType)
      return;

    // Compute the start offset for this page.
    long pageStart = PageHelper.GetPageStart(databaseHeader.PageSize, pageNumber);

    // Interior table cells contain [child page][rowid key].
    // Choose the first child whose key is >= target rowid.
    foreach (var pointer in cellPointers)
    {
      // Read the child page number from the cell.
      int childPage = ReadChildPageNumber(databaseFile, pageStart, pointer);

      // Read the key for this child.
      var (key, _) = VarintHelper.ReadVarint(databaseFile);
      // Descend into the first child whose key is >= target rowid.
      if (rowId <= (long)key)
      {
        PrintRowById(databaseFile, databaseHeader, table, query, childPage, rowId);
        return;
      }
    }

    // If rowid is greater than all keys, follow the right-most pointer.
    if (header.RightMostPointer.HasValue)
      PrintRowById(databaseFile, databaseHeader, table, query, (int)header.RightMostPointer.Value, rowId);
  }
}
