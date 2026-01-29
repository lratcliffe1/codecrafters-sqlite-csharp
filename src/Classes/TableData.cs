namespace codecrafters_sqlite.src.Classes;

public class TableData
{
  public required string Type { get; set; }
  public required string Name { get; set; }
  public required string TableName { get; set; }
  public required int RootPage { get; set; }
  public required List<Column> Columns { get; set; }
}

public class Column
{
  public required string Name { get; set; }
  public required string Type { get; set; }
  public required bool IsPrimaryKey { get; set; }
}