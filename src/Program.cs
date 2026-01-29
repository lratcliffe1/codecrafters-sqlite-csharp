using codecrafters_sqlite.src.Commands;

var (path, command) = args.Length switch
{
  0 => throw new InvalidOperationException("Missing <database path> and <command>"),
  1 => throw new InvalidOperationException("Missing <command>"),
  _ => (args[0], args[1])
};

FileStream? databaseFile = File.OpenRead(path);

if (command == ".dbinfo")
{
  DbInfo.Process(databaseFile);
}
else if (command == ".tables")
{
  TableInfo.Process(databaseFile);
}
else if (command.StartsWith("SELECT COUNT(*)", StringComparison.OrdinalIgnoreCase))
{
  SelectCount.Process(databaseFile, command);
}
else if (command.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase))
{
  SelectRows.Process(databaseFile, command);
}
else
{
  throw new InvalidOperationException($"Invalid command: {command}");
}