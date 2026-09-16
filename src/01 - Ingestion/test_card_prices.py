# ponytail: fetch_price_records/_to_price_record vivem dentro da célula
# "FUNÇÕES ESPECÍFICAS" do notebook (não é um módulo importável por si só),
# então isto carrega o código-fonte da célula direto do notebook .py e
# executa com um `requests` fake (índice de bulk-data + payload jsonl
# gzipado) - mesmo espírito de "exercitar o código real" de
# test_base_utils_get_secret.py, só que pra uma célula de notebook em vez de
# um módulo .py.

import gzip
import json
import os
import sys

_NB_PATH = os.path.join(os.path.dirname(__file__), "card_prices.py")
_MARKER = "FUNÇÕES ESPECÍFICAS DE CARD_PRICES"


def _load_functions(fake_get):
    cells = open(_NB_PATH, encoding="utf-8").read().split("# COMMAND ----------")
    cell_source = next(c for c in cells if _MARKER in c)

    ns = {
        "http_get_with_retry": lambda url, headers=None, timeout=30, retries=3: fake_get(url, headers=headers, timeout=timeout),
        "gzip": gzip,
        "json": json,
        "StructType": lambda fields: None,
        "StructField": lambda *a, **k: None,
        "StringType": lambda: None,
        "SCRYFALL_API_URL": "https://api.scryfall.test",
        "SCRYFALL_HEADERS": {},
        "SCRYFALL_BULK_TYPE": "oracle_cards",
        "MAX_RETRIES": 3,
    }
    exec(cell_source, ns)
    return ns["_to_price_record"], ns["fetch_price_records"]


class _Resp:
    def __init__(self, json_data=None, content=None):
        self._json = json_data
        self.content = content

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


def _fake_get_for(cards):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/bulk-data"):
            return _Resp(json_data={"data": [
                {"type": "oracle_cards", "jsonl_download_uri": "https://data.test/oracle.jsonl.gz"}
            ]})
        body = "\n".join(json.dumps(c) for c in cards).encode("utf-8")
        return _Resp(content=gzip.compress(body))
    return fake_get


def test_fetch_price_records_maps_fields():
    cards = [{
        "name": "Nissa, Worldsoul Speaker", "set": "drc", "rarity": "rare",
        "released_at": "2025-01-31",
        "prices": {"usd": "0.25", "eur": "0.21", "tix": "1.04"},
        "scryfall_uri": "https://scryfall.com/x",
        "image_uris": {"normal": "https://img/x.jpg"},
    }]

    _, fetch_price_records = _load_functions(_fake_get_for(cards))
    records = fetch_price_records()

    assert len(records) == 1
    assert records[0]["name"] == "Nissa, Worldsoul Speaker"
    assert records[0]["set"] == "drc"
    assert records[0]["usd"] == "0.25"
    assert records[0]["eur"] == "0.21"
    assert records[0]["tix"] == "1.04"
    assert records[0]["scryfall_uri"] == "https://scryfall.com/x"
    assert records[0]["image_url"] == "https://img/x.jpg"
    assert records[0]["releaseDate"] == "2025-01-31"


def test_double_faced_card_keeps_combined_name_as_is():
    # issue #<readequacao>: landing zone não tenta mais casar por nome com os
    # arquivos de `cards` (isso é join, fica pra Bronze/Silver) - o nome
    # combinado "A // B" que a Scryfall devolve pra cartas de dupla face é
    # gravado como veio, sem indexar por cada face separadamente.
    cards = [{
        "name": "Brightglass Gearhulk // Brightglass Gearhulk", "set": "eoe", "rarity": "mythic",
        "released_at": "2025-07-25",
        "prices": {"usd": "3.50", "eur": None, "tix": None},
        "scryfall_uri": "https://scryfall.com/y", "image_uris": None,
    }]

    _, fetch_price_records = _load_functions(_fake_get_for(cards))
    records = fetch_price_records()

    assert records[0]["name"] == "Brightglass Gearhulk // Brightglass Gearhulk"
    assert records[0]["usd"] == "3.50"
    assert records[0]["image_url"] is None


def test_fetch_price_records_returns_one_row_per_catalog_entry():
    cards = [
        {"name": "A", "released_at": "2020-01-01", "prices": {}},
        {"name": "B", "released_at": "2021-01-01", "prices": {}},
        {"name": "C", "released_at": "2022-01-01", "prices": {}},
    ]

    _, fetch_price_records = _load_functions(_fake_get_for(cards))
    records = fetch_price_records()

    assert [r["name"] for r in records] == ["A", "B", "C"]


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    test_fetch_price_records_maps_fields()
    test_double_faced_card_keeps_combined_name_as_is()
    test_fetch_price_records_returns_one_row_per_catalog_entry()
    print("OK")
