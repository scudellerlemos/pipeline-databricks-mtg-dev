# ponytail: exercita o get_secret() real de base_utils.py. dbutils não existe
# fora de um cluster Databricks, então a chamada dentro do try de get_secret
# levanta NameError - capturado pelo except genérico, exatamente o caminho de
# fallback que este teste quer cobrir.

import importlib.util
import os
import sys

_PATH = os.path.join(os.path.dirname(__file__), "base_utils.py")
_SPEC = importlib.util.spec_from_file_location("base_utils", _PATH)
base_utils = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(base_utils)


def test_explicit_default_wins():
    assert base_utils.get_secret("s3_bucket", default_value="s3://explicit") == "s3://explicit"


def test_falls_back_to_common_safe_default():
    assert base_utils.get_secret("catalog_name") == "mtg_dev"


def test_falls_back_to_layer_specific_default():
    assert base_utils.get_secret(
        "s3_gold_prefix", extra_safe_defaults={"s3_gold_prefix": "magic_the_gathering/gold"}
    ) == "magic_the_gathering/gold"


def test_raises_when_no_default_available():
    try:
        base_utils.get_secret("unknown_secret")
    except Exception as e:
        assert "unknown_secret" in str(e)
    else:
        raise AssertionError("expected Exception for secret with no default")


def test_s3_bucket_has_no_silent_fallback():
    # s3_bucket é o destino real de leitura/escrita de todas as camadas - não
    # pode cair silenciosamente num bucket placeholder quando o secret falha.
    try:
        base_utils.get_secret("s3_bucket")
    except Exception as e:
        assert "s3_bucket" in str(e)
    else:
        raise AssertionError("expected Exception for s3_bucket with no default")


def test_scope_padrao_e_o_de_dev():
    assert base_utils.secret_scope() == "mtg-pipeline"


def test_scope_vem_da_env_var():
    os.environ["MTG_SECRET_SCOPE"] = "mtg-pipeline-prd"
    try:
        assert base_utils.secret_scope() == "mtg-pipeline-prd"
    finally:
        del os.environ["MTG_SECRET_SCOPE"]


def test_catalog_name_nao_cai_pra_mtg_dev_fora_do_scope_de_dev():
    # O desastre que isso trava: dev e prd dividem o mesmo workspace, entao um
    # secret faltando no scope de prd fazia o job de producao gravar por cima
    # das tabelas de mtg_dev - em silencio, sem task vermelha.
    os.environ["MTG_SECRET_SCOPE"] = "mtg-pipeline-prd"
    try:
        base_utils.get_secret("catalog_name")
    except Exception as e:
        assert "catalog_name" in str(e)
    else:
        raise AssertionError("catalog_name nao pode cair pra mtg_dev fora de dev")
    finally:
        del os.environ["MTG_SECRET_SCOPE"]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_explicit_default_wins()
    test_falls_back_to_common_safe_default()
    test_falls_back_to_layer_specific_default()
    test_raises_when_no_default_available()
    test_s3_bucket_has_no_silent_fallback()
    test_scope_padrao_e_o_de_dev()
    test_scope_vem_da_env_var()
    test_catalog_name_nao_cai_pra_mtg_dev_fora_do_scope_de_dev()
    print("OK")
