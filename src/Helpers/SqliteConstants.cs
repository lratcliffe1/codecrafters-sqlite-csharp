namespace codecrafters_sqlite.src.Helpers;

public static class SqliteConstants
{
  public const int SchemaPageNumber = 1;
  public const int SchemaHeaderSize = 100;

  public const byte InteriorIndexPageType = 0x02;
  public const byte InteriorTablePageType = 0x05;
  public const byte LeafIndexPageType = 0x0A;
  public const byte LeafTablePageType = 0x0D;

  public const int InteriorPageHeaderSize = 12;
  public const int LeafPageHeaderSize = 8;

  public static bool IsInteriorPage(byte pageType)
  {
    return pageType == InteriorIndexPageType || pageType == InteriorTablePageType;
  }

  public static int GetPageHeaderSize(byte pageType)
  {
    return IsInteriorPage(pageType) ? InteriorPageHeaderSize : LeafPageHeaderSize;
  }
}
