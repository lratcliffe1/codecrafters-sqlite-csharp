namespace codecrafters_sqlite.src.Helpers;

public class ParsedSelectQuery
{
  public required string Verb { get; set; }
  public required List<string> Selected { get; set; }
  public required string TableName { get; set; }
  public required Func<Dictionary<string, string>, bool> WherePredicate { get; set; }
  public string? WhereColumn { get; set; }
  public string? WhereValue { get; set; }
}

public class ParsedIndexDefinition
{
  public required string OnTable { get; set; }
  public required string OnColumn { get; set; }
}

public static class SqlParseHelper
{
  public static ParsedSelectQuery ParseSelect(string command)
  {
    List<string> tokens = command
      .Replace(",", "")
      .Split(" ", StringSplitOptions.RemoveEmptyEntries)
      .ToList();

    int fromIndex = tokens.FindIndex(token => token.Equals("FROM", StringComparison.OrdinalIgnoreCase));
    int whereIndex = tokens.FindIndex(token => token.Equals("WHERE", StringComparison.OrdinalIgnoreCase));

    if (fromIndex == -1)
      throw new ArgumentException("Invalid SELECT: missing FROM");

    Func<Dictionary<string, string>, bool> wherePredicate = static _ => true;
    string? whereColumn = null;
    string? whereValue = null;

    if (whereIndex != -1)
    {
      string columnToCompare = tokens[whereIndex + 1];
      int equalsIndex = tokens.IndexOf("=");
      if (equalsIndex == -1)
        throw new ArgumentException("Invalid SELECT: missing '='");

      string valueToCompare = string.Join(" ", tokens[(equalsIndex + 1)..]).Trim('\'');

      wherePredicate = (row) => row[columnToCompare] == valueToCompare;
      whereColumn = columnToCompare;
      whereValue = valueToCompare;
    }

    return new ParsedSelectQuery
    {
      Verb = tokens.First(),
      Selected = tokens[1..fromIndex],
      TableName = tokens[fromIndex + 1],
      WherePredicate = wherePredicate,
      WhereColumn = whereColumn,
      WhereValue = whereValue
    };
  }

  public static ParsedIndexDefinition ParseIndexSql(string command)
  {
    command = string.Join(
      " ",
      command.Split((char[])null!, StringSplitOptions.RemoveEmptyEntries)
    );

    var lower = command.ToLowerInvariant();
    int onIndex = lower.IndexOf(" on ");
    if (onIndex == -1)
      throw new ArgumentException("Invalid index SQL: missing ON");

    string afterOn = command[(onIndex + 4)..].Trim();
    int openParen = afterOn.IndexOf('(');
    int closeParen = afterOn.IndexOf(')', openParen + 1);

    if (openParen == -1 || closeParen == -1)
      throw new ArgumentException("Invalid index SQL: missing (column)");

    string table = afterOn[..openParen].Trim();
    string column = afterOn[(openParen + 1)..closeParen].Trim();

    if (column.Contains(","))
      throw new NotSupportedException("Only single-column indexes are supported");

    return new ParsedIndexDefinition
    {
      OnTable = table,
      OnColumn = column
    };
  }
}
