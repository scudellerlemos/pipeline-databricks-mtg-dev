#!/usr/bin/env python3
"""
Script para deploy dos jobs do pipeline no Databricks.

Este repo nao usa Databricks Asset Bundles, entao nao ha interpolacao nativa
de ${resources.jobs.X.id} entre jobs. MTG_PIPELINE (orquestrador) referencia
MTG_STAGE/MTG_BRONZE/MTG_SILVER/MTG_GOLD via run_job_task.job_id - por isso
esses 4 tem que ser deployados ANTES, e seus job_ids substituidos nos
placeholders "{{MTG_STAGE_JOB_ID}}" etc. do pipeline.yml antes dele ser deployado.
"""

import yaml
import json
import subprocess
import sys
import os
from datetime import datetime

# (arquivo, job_key) - ordem importa: os 4 primeiros precisam existir antes
# do orquestrador (ultimo) ser deployado, pra gente saber os job_ids deles.
DEPLOY_ORDER = [
    (".github/DAGs/stage.yml", "MTG_STAGE"),
    (".github/DAGs/bronze.yml", "MTG_BRONZE"),
    (".github/DAGs/silver.yml", "MTG_SILVER"),
    (".github/DAGs/gold.yml", "MTG_GOLD"),
    (".github/DAGs/pipeline.yml", "MTG_PIPELINE"),
]

JSON_TMP = "job_deploy.json"


def log(message, level="INFO"):
    """Função para logging padronizado"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def load_job_config(yaml_path, job_key, job_ids_by_key=None):
    """Le o YAML, extrai o job e (se for o orquestrador) substitui os
    placeholders de job_id pelos IDs reais ja deployados."""
    with open(yaml_path, "r", encoding="utf-8") as f:
        yaml_data = yaml.safe_load(f)

    if "resources" not in yaml_data or "jobs" not in yaml_data["resources"]:
        raise ValueError(f"{yaml_path}: resources.jobs não encontrado")
    if job_key not in yaml_data["resources"]["jobs"]:
        raise ValueError(f"{yaml_path}: job {job_key} não encontrado")

    job_config = yaml_data["resources"]["jobs"][job_key]

    if job_ids_by_key:
        for task in job_config.get("tasks", []):
            if "run_job_task" not in task:
                continue
            placeholder = task["run_job_task"].get("job_id", "")
            for referenced_key, referenced_id in job_ids_by_key.items():
                token = "{{" + f"{referenced_key}_JOB_ID" + "}}"
                if placeholder == token:
                    task["run_job_task"]["job_id"] = referenced_id

    return job_config


def write_json(job_config):
    json_content = json.dumps(job_config, indent=2, ensure_ascii=False)
    with open(JSON_TMP, "w", encoding="utf-8") as f:
        f.write(json_content)
    return json_content


def get_existing_job_id(job_name, is_new_cli=False):
    """Obtém o ID do job existente pelo nome, se houver"""
    try:
        output_flag = "json" if is_new_cli else "JSON"
        result = subprocess.run(
            ["databricks", "jobs", "list", "--output", output_flag],
            capture_output=True,
            text=True,
            check=True,
        )
        jobs_data = json.loads(result.stdout)
        jobs_list = jobs_data if isinstance(jobs_data, list) else jobs_data.get("jobs", [])
        for job in jobs_list:
            if job.get("settings", {}).get("name") == job_name:
                job_id = job.get("job_id")
                log(f"✅ Job existente encontrado: {job_name} (ID {job_id})")
                return job_id
        log(f"ℹ️ Job {job_name} não existe ainda, será criado")
        return None
    except subprocess.CalledProcessError as e:
        log(f"⚠️ Erro ao listar jobs: {e.stderr}", "WARN")
        return None
    except Exception as e:
        log(f"⚠️ Erro inesperado ao listar jobs: {e}", "WARN")
        return None


def validate_databricks_connection():
    """Valida a conexão com o Databricks"""
    try:
        log("🔗 Testando conexão com Databricks...")
        result = subprocess.run(["databricks", "--version"], capture_output=True, text=True, check=True)
        version_output = result.stdout.strip()
        log(f"✅ Databricks CLI version: {version_output}")

        is_new_cli = version_output.startswith("Databricks CLI v")
        is_old_cli = version_output.startswith("Version ")

        if is_old_cli:
            try:
                subprocess.run(
                    ["databricks", "jobs", "configure", "--version", "2.1"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                log("✅ CLI antiga configurada para Jobs API 2.1")
            except subprocess.CalledProcessError as e:
                log(f"⚠️ Configuração falhou: {e.stderr}", "WARN")
                os.environ["DATABRICKS_JOBS_API_VERSION"] = "2.1"

        if is_new_cli:
            subprocess.run(["databricks", "workspace", "list", "/"], capture_output=True, text=True, check=True)
        else:
            subprocess.run(["databricks", "workspace", "list", "/"], capture_output=True, text=True, check=True)
        log("✅ Conexão com workspace estabelecida")

        return True, is_new_cli
    except subprocess.CalledProcessError as e:
        log(f"❌ Erro na conexão com Databricks (exit {e.returncode}): stdout={e.stdout!r} stderr={e.stderr!r}", "ERROR")
        return False, False
    except Exception as e:
        log(f"❌ Erro inesperado na validação: {e}", "ERROR")
        return False, False


def deploy_one_job(yaml_path, job_key, is_new_cli, job_ids_by_key):
    """Deploya (cria ou atualiza) um job e retorna seu job_id."""
    log(f"📖 Lendo {yaml_path} ({job_key})...")
    job_config = load_job_config(yaml_path, job_key, job_ids_by_key)
    write_json(job_config)

    existing_id = get_existing_job_id(job_key, is_new_cli)

    env = os.environ.copy()
    if not is_new_cli:
        env["DATABRICKS_JOBS_API_VERSION"] = "2.1"

    with open(JSON_TMP, "r", encoding="utf-8") as f:
        json_content = f.read()

    if existing_id:
        log(f"🔄 Atualizando job existente: {job_key} (ID {existing_id})")
        if is_new_cli:
            write_json({"job_id": existing_id, "new_settings": job_config})
            result = subprocess.run(
                ["databricks", "jobs", "reset", "--json", f"@{JSON_TMP}"],
                capture_output=True, text=True, check=True, env=env,
            )
        else:
            result = subprocess.run(
                ["databricks", "jobs", "reset", "--job-id", str(existing_id), "--json-file", JSON_TMP],
                capture_output=True, text=True, check=True, env=env,
            )
        job_id = existing_id
    else:
        log(f"🆕 Criando novo job: {job_key}")
        if is_new_cli:
            result = subprocess.run(
                ["databricks", "jobs", "create", "--json", f"@{JSON_TMP}"],
                capture_output=True, text=True, check=True, env=env,
            )
        else:
            result = subprocess.run(
                ["databricks", "jobs", "create", "--json-file", JSON_TMP],
                capture_output=True, text=True, check=True, env=env,
            )
        try:
            job_id = json.loads(result.stdout).get("job_id")
        except Exception:
            job_id = None

    log(f"📄 Resposta do Databricks para {job_key}: {result.stdout.strip()[:300]}")
    if job_id is None:
        raise RuntimeError(f"Não foi possível determinar o job_id de {job_key} após o deploy")

    log(f"🎯 {job_key} -> job_id {job_id}")
    return job_id


def deploy_all():
    connection_success, is_new_cli = validate_databricks_connection()
    if not connection_success:
        return False

    job_ids_by_key = {}
    try:
        for yaml_path, job_key in DEPLOY_ORDER:
            job_id = deploy_one_job(yaml_path, job_key, is_new_cli, job_ids_by_key)
            job_ids_by_key[job_key] = job_id
        return True
    except subprocess.CalledProcessError as e:
        log(f"❌ Erro no deploy: {e}", "ERROR")
        log(f"📄 stdout: {e.stdout}", "DEBUG")
        log(f"📄 stderr: {e.stderr}", "ERROR")
        return False
    except Exception as e:
        log(f"❌ Erro inesperado: {e}", "ERROR")
        return False


def verify_deployment():
    """Verifica se todos os jobs foram deployados"""
    try:
        log("🔍 Verificando deploy...")
        sleep_time = 30
        log(f"⏳ Aguardando {sleep_time} segundos para verificação...")
        import time
        time.sleep(sleep_time)

        result = subprocess.run(
            ["databricks", "jobs", "list", "--output", "JSON"], capture_output=True, text=True, check=True,
        )
        jobs_data = json.loads(result.stdout)
        jobs_list = jobs_data if isinstance(jobs_data, list) else jobs_data.get("jobs", [])
        names_found = {job.get("settings", {}).get("name") for job in jobs_list}

        all_ok = True
        for _, job_key in DEPLOY_ORDER:
            if job_key in names_found:
                log(f"✅ {job_key} verificado")
            else:
                log(f"❌ {job_key} não encontrado após deploy", "ERROR")
                all_ok = False
        return all_ok
    except Exception as e:
        log(f"❌ Erro na verificação: {e}", "ERROR")
        return False


def cleanup():
    """Limpa arquivos temporários"""
    try:
        if os.path.exists(JSON_TMP):
            os.remove(JSON_TMP)
            log("🧹 Arquivo temporário removido")
    except Exception as e:
        log(f"⚠️ Erro na limpeza: {e}", "WARN")


if __name__ == "__main__":
    log("🚀 Iniciando deploy do pipeline...")
    log("=" * 60)

    try:
        deploy_success = deploy_all()

        if deploy_success:
            log("✅ Deploy executado com sucesso!")
            verify_success = verify_deployment()

            if verify_success:
                log("🎉 Deploy e verificação concluídos com sucesso!")
                log("=" * 60)
                sys.exit(0)
            else:
                log("⚠️ Deploy executado mas verificação falhou", "WARN")
                log("=" * 60)
                sys.exit(1)
        else:
            log("💥 Falha no deploy!", "ERROR")
            log("=" * 60)
            sys.exit(1)

    except KeyboardInterrupt:
        log("⚠️ Deploy interrompido pelo usuário", "WARN")
        sys.exit(1)
    except Exception as e:
        log(f"💥 Erro crítico: {e}", "ERROR")
        sys.exit(1)
    finally:
        cleanup()
