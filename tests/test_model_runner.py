import pytest
from watchtower import model_runner as mr

SCHEMA = {'type':'object','properties':{'ok':{'type':'boolean'}},'required':['ok'],'additionalProperties':False}
REQUEST = {'profile':'deep','system':'Return JSON.','prompt':'Synthetic input','tools':'none','json_schema':SCHEMA}
FIRST={'engine':'claude','model':'claude-opus-4-6','effort':'high'}
SECOND={'engine':'claude','model':'claude-sonnet-4-6','effort':'medium'}

@pytest.fixture(autouse=True)
def policy(monkeypatch):
    monkeypatch.setattr(mr.config,'model_profile',lambda n:{'models':[FIRST,SECOND]})
    monkeypatch.setattr(mr.config,'worker_fallback_policy',lambda:{'enabled':True})

def test_quota_fallback_bounded(monkeypatch):
    calls=[]
    def attempt(c,*a):
        calls.append(c)
        return (None,True) if c==FIRST else ({'structured_output':{'ok':True}},False)
    monkeypatch.setattr(mr,'_attempt',attempt)
    result=mr.run(REQUEST)
    assert calls==[FIRST,SECOND]
    assert result['execution']['profile']=='deep'
    assert result['execution']['attempts'][0]['status']=='quota_exhausted'

def test_all_models_exhausted(monkeypatch):
    calls=[]
    monkeypatch.setattr(mr,'_attempt',lambda c,*a:(calls.append(c) or None,True))
    with pytest.raises(mr.ModelRunError,match='all configured'):mr.run(REQUEST)
    assert calls==[FIRST,SECOND]

def test_nonquota_no_retry(monkeypatch):
    calls=[]
    monkeypatch.setattr(mr,'_attempt',lambda c,*a:(calls.append(c) or None,False))
    with pytest.raises(mr.ModelRunError,match='only applies to quota'):mr.run(REQUEST)
    assert calls==[FIRST]

@pytest.mark.parametrize('policy',[None,{'enabled':False}])
def test_disabled_no_retry(monkeypatch,policy):
    monkeypatch.setattr(mr.config,'model_profile',lambda n:{'models':[FIRST,SECOND]})
    monkeypatch.setattr(mr.config,'worker_fallback_policy',lambda:policy)
    calls=[]
    monkeypatch.setattr(mr,'_attempt',lambda c,*a:(calls.append(c) or None,True))
    with pytest.raises(mr.ModelRunError):mr.run(REQUEST)
    assert calls==[FIRST]

@pytest.mark.parametrize('patch',[{'model':'private'},{'tools':['Read']},{'timeout_seconds':481},{'max_budget_usd':6},{'max_output_bytes':2097153},{'json_schema':None}])
def test_invalid_request_no_launch(monkeypatch,patch):
    monkeypatch.setattr(mr,'_attempt',lambda *a:pytest.fail('must not launch'))
    with pytest.raises(mr.ModelRunError):mr.run({**REQUEST,**patch})

def test_invalid_schema_output_no_retry(monkeypatch):
    calls=[]
    monkeypatch.setattr(mr,'_attempt',lambda c,*a:(calls.append(c) or {'structured_output':{'ok':'wrong'}},False))
    with pytest.raises(mr.ModelRunError,match='schema mismatch'):mr.run(REQUEST)
    assert len(calls)==1

def test_capture_output_limit(tmp_path):
    import sys
    with pytest.raises(mr.ModelRunError,match='output limit'):
        mr._capture([sys.executable,'-c','print("x"*2000)'],'',tmp_path,2,1024,{})

def test_capture_timeout(tmp_path):
    import sys
    with pytest.raises(mr.ModelRunError,match='timed out'):
        mr._capture([sys.executable,'-c','import time;time.sleep(10)'],'',tmp_path,.1,1024,{})

def test_codex_unsupported_tools_fails_closed(tmp_path,monkeypatch):
    monkeypatch.setattr(mr,'_capture',lambda *a:pytest.fail('must not launch unsupported provider'))
    with pytest.raises(mr.ModelRunError,match='tools:none execution is unavailable'):
        mr._attempt({**SECOND,'engine':'codex'},REQUEST,tmp_path,1,1024)

def test_claude_restrictions_and_budget(tmp_path,monkeypatch):
    import json
    calls=[]
    def capture(argv,*a):
        calls.append(argv)
        return 0,json.dumps({'subtype':'success','is_error':False,'structured_output':{'ok':True}}),''
    monkeypatch.setattr(mr,'_capture',capture)
    result,quota=mr._attempt(FIRST,{**REQUEST,'max_budget_usd':5},tmp_path,1,1024)
    assert result and not quota
    argv=calls[0]
    assert argv[argv.index('--tools')+1]==''
    assert '--strict-mcp-config' in argv and '--no-session-persistence' in argv
    assert argv[argv.index('--max-budget-usd')+1]=='5'


def test_unsupported_profile_rejected_before_any_prompt(monkeypatch):
    monkeypatch.setattr(mr.config,'model_profile',lambda n:{'models':[FIRST,{**SECOND,'engine':'codex'}]})
    monkeypatch.setattr(mr,'_attempt',lambda *a:pytest.fail('must fail before provider invocation'))
    with pytest.raises(mr.ModelRunError,match='profile_capability_unavailable: codex'):
        mr.run(REQUEST)


def test_unsupported_schema_rejected_before_launch(monkeypatch):
    monkeypatch.setattr(mr,'_attempt',lambda *a:pytest.fail('must not launch'))
    with pytest.raises(mr.ModelRunError,match='unsupported JSON schema'):
        mr.run({**REQUEST,'json_schema':{'$ref':'private'}})
