# ponytail: apply_target e o unico ponto onde dev e prd se diferenciam. Os
# YAMLs sao identicos pros dois alvos de proposito (o repo de prd nao tem copia
# de codigo, so faz checkout deste repo numa tag), entao se esta funcao errar o
# job de producao sobe apontando pro catalogo de desenvolvimento e grava
# em cima de mtg_dev sem ninguem perceber.

import copy
import importlib.util
import os
import sys

_PATH = os.path.join(os.path.dirname(__file__), "deploy.py")

ALVO_PRD = {
    # knobs do deploy - mexem no job, nao chegam no cluster
    "MTG_JOB_SUFFIX": "_PRD",
    "MTG_GIT_URL": "https://github.com/scudellerlemos/pipeline-databricks-mtg-dev",
    "MTG_GIT_TAG": "v1.0.0",
    "MTG_PAUSE_STATUS": "UNPAUSED",
    # config do ambiente - vai pro cluster, onde get_secret le
    "MTG_ENVIRONMENT": "production",
    "MTG_CATALOG_NAME": "mtg_prod",
    "MTG_S3_STAGE_PREFIX": "prod/stage",
}

JOB_BASE = {
    "name": "MTG_GOLD",
    "tasks": [{"task_key": "gold_mercado_cartas"}],
    "job_clusters": [
        {
            "job_cluster_key": "Job_cluster",
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            },
        }
    ],
    "git_source": {
        "git_url": "https://github.com/scudellerlemos/pipeline-databricks-mtg-dev",
        "git_provider": "gitHub",
        "git_branch": "main",
    },
    "schedule": {"quartz_cron_expression": "0 0 6 ? * 2#1", "pause_status": "UNPAUSED"},
    "tags": {"environment": "development"},
}


def _deploy_com_env(env):
    """Recarrega deploy.py com um ambiente - TARGET e lido no import.

    Limpa TODA env var MTG_* antes, nao so as do alvo: o
    CONFIG_DO_AMBIENTE varre o prefixo inteiro, entao uma MTG_* solta na
    maquina de quem roda o teste vazaria pro spark_env_vars.
    """
    antigo = {k: v for k, v in os.environ.items() if k.startswith("MTG_")}
    for k in antigo:
        del os.environ[k]
    os.environ.update(env)
    try:
        spec = importlib.util.spec_from_file_location("deploy_sob_teste", _PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        for k in [k for k in os.environ if k.startswith("MTG_")]:
            del os.environ[k]
        os.environ.update(antigo)


def test_sem_env_o_job_fica_exatamente_como_esta_no_yaml():
    deploy = _deploy_com_env({})
    job = deploy.apply_target(copy.deepcopy(JOB_BASE))

    assert job["name"] == "MTG_GOLD"
    assert job["git_source"]["git_branch"] == "main"
    assert "git_tag" not in job["git_source"]
    assert job["tags"]["environment"] == "development"
    # sem config injetada, get_secret no notebook cai no scope compartilhado
    assert job["job_clusters"][0]["new_cluster"]["spark_env_vars"] == {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    }


def test_alvo_prd_renomeia_e_injeta_a_config():
    deploy = _deploy_com_env(ALVO_PRD)
    job = deploy.apply_target(copy.deepcopy(JOB_BASE))

    assert job["name"] == "MTG_GOLD_PRD"
    env_vars = job["job_clusters"][0]["new_cluster"]["spark_env_vars"]
    assert env_vars["MTG_CATALOG_NAME"] == "mtg_prod"
    assert env_vars["MTG_S3_STAGE_PREFIX"] == "prod/stage"
    # arma a trava de catalogo do get_secret no cluster
    assert env_vars["MTG_ENVIRONMENT"] == "production"
    # injetar nao pode comer o que ja estava no cluster
    assert env_vars["PYSPARK_PYTHON"] == "/databricks/python3/bin/python3"
    assert job["tags"]["environment"] == "production"


def test_knobs_do_deploy_nao_vazam_pro_cluster():
    # git_tag e sufixo de nome mexem no JOB. Mandar pro cluster so polui o
    # ambiente do notebook com coisa que ele nunca le.
    deploy = _deploy_com_env(ALVO_PRD)
    job = deploy.apply_target(copy.deepcopy(JOB_BASE))

    env_vars = job["job_clusters"][0]["new_cluster"]["spark_env_vars"]
    for knob in ("MTG_JOB_SUFFIX", "MTG_GIT_URL", "MTG_GIT_TAG", "MTG_PAUSE_STATUS"):
        assert knob not in env_vars


def test_tag_substitui_branch_e_nunca_convivem():
    # git_source aceita branch OU tag - mandar os dois e erro 400 da API, e
    # producao tem que rodar um ref imutavel, nao uma branch que anda sozinha.
    deploy = _deploy_com_env(ALVO_PRD)
    job = deploy.apply_target(copy.deepcopy(JOB_BASE))

    assert job["git_source"]["git_tag"] == "v1.0.0"
    assert "git_branch" not in job["git_source"]


def test_pause_status_do_alvo_vence_o_yaml():
    # jobs reset sobrescreve as settings inteiras, entao pausar o schedule de
    # dev pela UI nao sobrevive ao proximo deploy - so o knob de alvo segura.
    deploy = _deploy_com_env({**ALVO_PRD, "MTG_PAUSE_STATUS": "PAUSED"})
    job = deploy.apply_target(copy.deepcopy(JOB_BASE))

    assert job["schedule"]["pause_status"] == "PAUSED"


def test_orquestrador_sem_cluster_e_sem_git_nao_quebra():
    # MTG_PIPELINE so tem run_job_task: nao tem job_clusters nem git_source.
    deploy = _deploy_com_env(ALVO_PRD)
    job = deploy.apply_target({"name": "MTG_PIPELINE", "tasks": [{"task_key": "rodar_stage"}]})

    assert job["name"] == "MTG_PIPELINE_PRD"


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_sem_env_o_job_fica_exatamente_como_esta_no_yaml()
    test_alvo_prd_renomeia_e_injeta_a_config()
    test_knobs_do_deploy_nao_vazam_pro_cluster()
    test_tag_substitui_branch_e_nunca_convivem()
    test_pause_status_do_alvo_vence_o_yaml()
    test_orquestrador_sem_cluster_e_sem_git_nao_quebra()
    print("OK")
