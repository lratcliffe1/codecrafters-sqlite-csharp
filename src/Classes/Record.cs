namespace codecrafters_sqlite.src.Classes;

public class Record
{
  public required string Type { get; set; }
  public required string Name { get; set; }
  public required string TableName { get; set; }
  public required int RootPage { get; set; }
  public required string Sql { get; set; }
}