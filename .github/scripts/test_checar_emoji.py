import importlib.util
import os

_ESPEC = importlib.util.spec_from_file_location("checar_emoji", os.path.join(os.path.dirname(__file__), "checar_emoji.py"))
checar_emoji = importlib.util.module_from_spec(_ESPEC)
_ESPEC.loader.exec_module(checar_emoji)


def test_acha_emoji_com_linha():
    assert checar_emoji.achar_emojis("ok\n- ✅ passou\n\U0001F680 deploy") == [(2, "✅"), (3, "\U0001F680")]


def test_setas_e_simbolos_de_texto_passam():
    assert checar_emoji.achar_emojis("Stage → Bronze, ≥ 1, 2×3, ação") == []
