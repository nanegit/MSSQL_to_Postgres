from infrastructure.factory import MigrationServiceFactory


def main():
    print("="*70)
    print("MSSQL TO POSTGRESQL MIGRATION TOOL")
    print("="*70)
    print()
    
    # Կոնֆիգուրացիա
    mssql_config = {
    'server': r'DESKTOP-0HV19S3\SQLEXPRESS',  
    'database': 'Union',                   
    'username': 'DESKTOP-0HV19S3\\intel', 
    'trusted_connection': 'yes' 
}

    
    pg_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'Union',
        'username': 'postgres',
        'password': 'nane.2004'
    }
    
    try:
        migration_service = MigrationServiceFactory.create(
            mssql_config=mssql_config,
            pg_config=pg_config,
            batch_size=10000
        )
        
        stats = migration_service.execute()
        
        print(f"\n{'='*70}")
        print("FINAL STATISTICS")
        print(f"{'='*70}")
        print(f"✓ Tables: {stats.tables_processed}")
        print(f"✓ Rows: {stats.total_rows:,}")
        print(f"✓ Failed: {len(stats.failed_tables)}")
        
        return 0 if len(stats.failed_tables) == 0 else 1
        
    except Exception as e:
        print(f"\n✗ FATAL ERROR: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
