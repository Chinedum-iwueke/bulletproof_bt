import io
import json
from types import SimpleNamespace

import pytest

from scripts import run_native_diagnostic as cli
from tests.governance.test_diagnostic_tools import setup, SOURCE


def configure(monkeypatch, raw, dirty=''):
    monkeypatch.setattr(cli.subprocess, 'check_output',
        lambda args, **kw: SOURCE if 'rev-parse' in args else dirty)
    monkeypatch.setattr(cli.sys, 'stdin', SimpleNamespace(buffer=io.BytesIO(raw)))


def test_cli_emits_bound_receipt(monkeypatch, capsys):
    request, catalog = setup()
    configure(monkeypatch, json.dumps(dict(request=request, evidence_catalog=catalog)).encode())
    cli.main()
    receipt = json.loads(capsys.readouterr().out)
    assert receipt['source_commit'] == SOURCE
    assert receipt['execution_authority'] is False


@pytest.mark.parametrize('raw,dirty,match', [
    (b'{}', ' M src/changed.py', 'clean pinned'),
    (b'x'*262145, '', 'exceeds bound'),
    (b'{"request":{},"request":{}}', '', 'duplicate'),
    (b'{}', '', 'envelope'),
])
def test_cli_rejects_invalid_input_before_execution(monkeypatch, raw, dirty, match):
    configure(monkeypatch, raw, dirty)
    with pytest.raises(ValueError, match=match):
        cli.main()
