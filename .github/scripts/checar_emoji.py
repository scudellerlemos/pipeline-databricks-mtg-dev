#!/usr/bin/env python3
"""Falha se algum arquivo versionado tiver emoji (código, docs, YAML)."""
import re
import subprocess
import sys
from pathlib import Path

RAIZ_REPO = Path(__file__).resolve().parents[2]
# Pictogramas, símbolos diversos/dingbats (check, X, alerta) e o seletor de variação.
# Setas e símbolos de texto (→, ≥, ×) ficam de fora: são usados na doc.
REGEX_EMOJI = re.compile("[\U0001F000-\U0001FAFF\u2600-\u27BF\u2B00-\u2BFF\uFE0F]")


def achar_emojis(texto):
    """[(linha, emoji)] de cada emoji no texto."""
    return [(n, m.group()) for n, linha in enumerate(texto.splitlines(), 1) for m in REGEX_EMOJI.finditer(linha)]


def main():
    arquivos = subprocess.run(["git", "ls-files", "-z"], cwd=RAIZ_REPO, capture_output=True, text=True, check=True).stdout.split("\0")
    achados = []
    for arquivo in filter(None, arquivos):
        try:
            texto = (RAIZ_REPO / arquivo).read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError):
            continue  # binário (imagem) ou removido no working tree
        achados += [f"{arquivo}:{n}: {e}" for n, e in achar_emojis(texto)]
    if achados:
        print("Emoji encontrado - tire antes do merge:\n" + "\n".join(achados))
        sys.exit(1)
    print("OK: nenhum emoji nos arquivos versionados")


if __name__ == "__main__":
    main()
