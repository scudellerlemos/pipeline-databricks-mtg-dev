# Testa a condição de merge por coluna_chave de salvar_na_gold (gold_utils.py).
# O módulo precisa de pyspark/Databricks, então a lógica é espelhada aqui.


def montar_plano_merge(coluna_chave):
    colunas_chave = [coluna_chave] if isinstance(coluna_chave, str) else list(coluna_chave)
    # <=> (null-safe): com "=", chave NULL nunca casa e seria reinserida a cada execução.
    condicao_merge = " AND ".join(f"gold.{k} <=> novo.{k}" for k in colunas_chave)
    return colunas_chave, condicao_merge


def test_chave_composta_mercado_cartas():
    colunas_chave, condicao = montar_plano_merge(["ID_CARTA", "DT_COTACAO"])
    assert colunas_chave == ["ID_CARTA", "DT_COTACAO"]
    assert condicao == "gold.ID_CARTA <=> novo.ID_CARTA AND gold.DT_COTACAO <=> novo.DT_COTACAO"


def test_coluna_chave_unica_string():
    colunas_chave, condicao = montar_plano_merge("ID_CARTA")
    assert colunas_chave == ["ID_CARTA"]
    assert condicao == "gold.ID_CARTA <=> novo.ID_CARTA"


def montar_condicao_update(colunas_lote, colunas_chave, campos_atuais):
    """Espelho de gold_utils.montar_condicao_update."""
    if not set(colunas_lote) <= set(campos_atuais):
        return None
    return " OR ".join(
        f"NOT (gold.{c} <=> novo.{c})" for c in colunas_lote if c not in colunas_chave
    ) or None


def test_update_so_quando_algum_valor_muda():
    condicao = montar_condicao_update(
        ["ID_CARTA", "DT_COTACAO", "VLR_USD", "ID_CARTA_CANONICO"],
        ["ID_CARTA", "DT_COTACAO"],
        {"ID_CARTA": "string", "DT_COTACAO": "timestamp", "VLR_USD": "double", "ID_CARTA_CANONICO": "string"},
    )
    # <=>: NULL -> valor tambem conta como mudanca.
    assert condicao == "NOT (gold.VLR_USD <=> novo.VLR_USD) OR NOT (gold.ID_CARTA_CANONICO <=> novo.ID_CARTA_CANONICO)"


def test_coluna_nova_atualiza_tudo():
    # gold.COL_NOVA nao existe na tabela; sem condicao o historico recebe a coluna.
    assert montar_condicao_update(["ID_CARTA", "COL_NOVA"], ["ID_CARTA"], {"ID_CARTA": "string"}) is None


def test_so_chave_sem_condicao():
    assert montar_condicao_update(["ID_CARTA"], ["ID_CARTA"], {"ID_CARTA": "string"}) is None


if __name__ == "__main__":
    test_update_so_quando_algum_valor_muda()
    test_coluna_nova_atualiza_tudo()
    test_so_chave_sem_condicao()
    test_chave_composta_mercado_cartas()
    test_coluna_chave_unica_string()
    print("OK")
