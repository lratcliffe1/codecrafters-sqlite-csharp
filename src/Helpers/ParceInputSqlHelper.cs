namespace codecrafters_sqlite.src.Helpers;

public class ParsedInput()
{
  public required string BaseCommand { get; set; }
  public required List<string> Selected { get; set; }
  public required string TableName { get; set; }

}

public static class ParceInputSqlHelper
{
  public static ParsedInput ParseInput(string command)
  {
    var splitCommand = command
      .ToLower()
      .Replace(",", "")
      .Split(" ")
      .ToList();

    var baseCommand = splitCommand.First();
    var selected = splitCommand[(splitCommand.IndexOf(baseCommand) + 1)..splitCommand.IndexOf("from")];
    var tableName = splitCommand.Last();

    return new ParsedInput()
    {
      BaseCommand = baseCommand,
      Selected = selected,
      TableName = tableName,
    };
  }
}