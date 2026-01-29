
namespace codecrafters_sqlite.src.Classes;

public class BTreePageHeader
{
  // 2 (0x02) interior index b-tree page.
  // 5 (0x05) interior table b-tree page.
  // 10 (0x0a) leaf index b-tree page.
  // 13 (0x0d) leaf table b-tree page.
  public byte PageType { get; set; } // interior index b-tree or interior table b-tree or leaf index b-tree or leaf table b-tree
  public ushort FirstFreeblock { get; set; }
  public ushort CellCount { get; set; }
  public uint CellContentStart { get; set; } // uint to handle the 65536 case
  public byte FragmentedFreeBytes { get; set; }
  public uint? RightMostPointer { get; set; } // Nullable because it's optional
}
