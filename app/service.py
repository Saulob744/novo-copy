import time
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from app import db_utils
from app import main 

def run_copy(source_url, dest_url, selected_tables=None, custom_query=None, chunk_size=1000):
    dst_engine = None 
    try:
        start_time = time.time()
        main.PROGRESSO = {"message": "🔍 Verificando banco de destino...", "progress": "2%", "running": True, "error": None}

        # --- LÓGICA DE CRIAÇÃO DO BANCO (IF NOT EXISTS) ---
        # --- LÓGICA DE CRIAÇÃO DO BANCO (IF NOT EXISTS) + LIMPEZA ---
        try:
            # Tenta uma conexão rápida só para ver se o banco existe
            temp_check = create_engine(dest_url)
            with temp_check.connect() as conn:
                pass
        except Exception:
            # Se falhou, o banco não existe.
            main.PROGRESSO["message"] = "🔨 Criando banco e limpando lixo..."
            
            base_url, db_name = dest_url.rsplit('/', 1)
            postgres_url = f"{base_url}/postgres"
            
            # 1. Cria o banco de dados
            engine_admin = create_engine(postgres_url, isolation_level="AUTOCOMMIT")
            with engine_admin.connect() as conn:
                conn.execute(text(f'CREATE DATABASE "{db_name}"'))
            print(f"✨ Banco '{db_name}' criado.")

            # 2. Conecta no banco NOVO para deletar o schema public
           
            engine_new_db = create_engine(dest_url, isolation_level="AUTOCOMMIT")
            with engine_new_db.connect() as conn:
                # O CASCADE garante que se houver algo (mesmo vazio), ele remove tudo
                conn.execute(text('DROP SCHEMA IF EXISTS public CASCADE'))
                
            
            print(f"🗑️ Schema 'public' removido de '{db_name}'.")
        # ------------------------------------------------------------
        # --------------------------------------------------

        main.PROGRESSO = {"message": "🔌 Conectando aos bancos...", "progress": "5%", "running": True, "error": None}
        src = db_utils.connect(source_url)
        dst = db_utils.connect(dest_url)
        dst_engine = dst 

        db_utils.set_replication_mode(dst, 'replica')

        # Fluxo de Tabelas
        schemas = db_utils.get_user_schemas(src)
        total_rows_all = 0

        main.PROGRESSO["message"] = "🔍 Analisando volumes..."
        for schema in schemas:
            tables = db_utils.get_tables(src, schema)
            for table in tables:
                if selected_tables and table not in selected_tables:
                    continue
                with src.connect() as conn:
                    total = conn.execute(sa.text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar()
                    total_rows_all += total

        processed = 0
        for schema in schemas:
            tables = db_utils.get_tables(src, schema)
            db_utils.copy_schema(src, dst, schema) 

            for table in tables:
                if selected_tables and table not in selected_tables:
                    continue

                for chunk in db_utils.fetch_rows_streaming(src, table, schema, chunk_size):
                    rows = [dict(r) for r in chunk] 
                    db_utils.insert_rows(dst, table, schema, rows)
                    processed += len(rows)
                    
                    percent = (processed / total_rows_all) * 100 if total_rows_all > 0 else 0
                    
                    try:
                        nome_exibicao = f"{schema}.{table}".encode('utf-8', 'replace').decode('utf-8')
                    except:
                        nome_exibicao = "tabela_config"

                    main.PROGRESSO = {
                        "message": f"📦 Copiando {nome_exibicao} ({processed}/{total_rows_all})",
                        "progress": f"{percent:.1f}%",
                        "running": True,
                        "error": None
                    }

        main.PROGRESSO = {"message": "✅ Finalizado!", "progress": "100%", "running": False, "error": None}

    except Exception as e:
        error_msg = str(e)
        print(f"❌ ERRO NO SERVICE: {error_msg}")
        main.PROGRESSO = {"message": "❌ Erro", "progress": "0%", "running": False, "error": error_msg}

    finally:
        if dst_engine:
            try:
                db_utils.set_replication_mode(dst_engine, 'origin')
                print("🔒 Modo de replicação restaurado.")
            except:
                pass