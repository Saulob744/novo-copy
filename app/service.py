import time
from app import db_utils
import sqlalchemy as sa


def run_copy(source_url, dest_url, selected_tables=None, custom_query=None, chunk_size=1000):
    try:
        start_time = time.time()

        print("🔌 Conectando bancos...")
        src = db_utils.connect(source_url)
        dst = db_utils.connect(dest_url)

        db_utils.set_replication_mode(dst, 'replica')

        # ========================
        # QUERY CUSTOMIZADA
        # ========================
        if custom_query:
            print("⚡ Executando query customizada...")

            with src.connect() as conn_src, dst.begin() as conn_dst:
                result = conn_src.execution_options(stream_results=True).execute(
                    sa.text(custom_query)
                )

                total = 0

                while True:
                    chunk = result.fetchmany(chunk_size)
                    if not chunk:
                        break

                    rows = [dict(row._mapping) for row in chunk]

                    print(f"Inserindo {len(rows)} registros...")
                    total += len(rows)

                print(f"✅ Finalizado! Total: {total}")

            return

        # ========================
        # FLUXO NORMAL
        # ========================
        schemas = db_utils.get_user_schemas(src)

        total_rows_all = 0

        print("🔍 Contando registros...")
        for schema in schemas:
            tables = db_utils.get_tables(src, schema)

            for table in tables:
                if selected_tables and table not in selected_tables:
                    continue

                with src.connect() as conn:
                    total = conn.execute(
                        sa.text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                    ).scalar()

                    total_rows_all += total

        processed = 0

        for schema in schemas:
            tables = db_utils.get_tables(src, schema)

            db_utils.copy_schema(src, dst, schema)

            for table in tables:
                if selected_tables and table not in selected_tables:
                    continue

                print(f"📦 Copiando {schema}.{table}...")

                for chunk in db_utils.fetch_rows_streaming(src, table, schema, chunk_size):
                    rows = [dict(r._mapping) for r in chunk]

                    db_utils.insert_rows(dst, table, schema, rows)

                    processed += len(rows)

                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    remaining = (total_rows_all - processed) / rate if rate > 0 else 0

                    print(f"{processed}/{total_rows_all} | ETA: {remaining:.1f}s")

        db_utils.set_replication_mode(dst, 'origin')

        print("✅ Processo finalizado com sucesso!")

    except Exception as e:
        print(f"❌ ERRO: {e}")