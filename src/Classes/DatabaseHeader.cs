namespace codecrafters_sqlite.src.Classes;

public class DatabaseHeader
{
  public string MagicHeaderString { get; set; } = string.Empty; // 53 51 4c 69 74 65 20 66 6f 72 6d 61 74 20 33 00 -> "SQLite format 3"
  public uint PageSize { get; set; } // page size in bytes. 0x00 0x01 = 65536
  public byte FileFormatWriteVersion { get; set; } // 1 for rollback journalling modes and 2 for WAL journalling mode. >2 read-only
  public byte FileFormatReadVersion { get; set; } // 1 for rollback journalling modes and 2 for WAL journalling mode. >2 cannot read or write
  public byte ReservedSpace { get; set; } // use by extensions i.e. nonce and/or cryptographic checksum
  public byte MaxPayloadFraction { get; set; } // 64
  public byte MinPayloadFraction { get; set; } // 32
  public byte LeafPayloadFraction { get; set; } // 32
  public uint FileChangeCounter { get; set; } // incremented whenever the database file is unlocked after having been modified
  public uint DatabaseSizeInPages { get; set; }
  public uint FirstFreelistTrunkPage { get; set; } // first unused page of the freelist
  public uint TotalFreelistPages { get; set; }
  public uint SchemaCookie { get; set; } // incremented whenever the database schema changes
  public uint SchemaFormatNumber { get; set; } // refers to the high-level SQL formatting
  public uint DefaultPageCacheSize { get; set; } // a suggestion
  public uint LargestRootBTreePage { get; set; } // linked to IncrementalVacuumMode
  public uint DatabaseTextEncoding { get; set; } // encoding used for all text strings stored in the database
  public uint UserVersion { get; set; }
  public uint IncrementalVacuumMode { get; set; } // ?
  public uint ApplicationId { get; set; } // identify the database as belonging to or associated with a particular application
  public uint VersionValidFor { get; set; } // value FileChangeCounter when SqliteVersionNumber stored 
  public uint SqliteVersionNumber { get; set; } // the SQLite library that most recently modified the database file
}