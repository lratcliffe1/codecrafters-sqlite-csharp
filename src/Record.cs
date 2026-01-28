namespace codecrafters_sqlite.src;

public class Record
{
  public required string Type { get; set; }
  public required string Name { get; set; }
  public required string TableName { get; set; }
  public required byte RootPage { get; set; }
}