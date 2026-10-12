import json
from pathlib import Path
import subprocess
import sys
from tests.support import Workspace

class ReplayTests(Workspace):
    def command(self,*extra):
        return subprocess.run([sys.executable,'-m','pullshit','--root',str(self.root),'replay',str(self.fixture),
                               '--database','data/replay/check/state.sqlite3','--account','900001',
                               '--source-group','900101','--target-group','900201',*extra],
                              text=True,capture_output=True,timeout=10)
    def setUp(self):
        super().setUp(); self.fixture=self.root/'fixture.jsonl'
        base={'post_type':'message','message_type':'group','self_id':900001,'user_id':900401,'group_id':900101}
        rows=[{**base,'time':100,'message_id':1,'message':[{'type':'text','data':{'text':'有趣的离线回放内容，验证选择、发送和精确反馈'}}]},
              {**base,'time':101,'message_id':2,'message':[{'type':'reply','data':{'id':'1'}},{'type':'text','data':{'text':'/搬'}}]},
              {**base,'time':102,'group_id':900201,'message_id':3,'message':[{'type':'reply','data':{'id':'1001'}},{'type':'text','data':{'text':'/喜欢'}}]}]
        self.fixture.write_text('\n'.join(json.dumps(row,ensure_ascii=False) for row in rows))
    def test_cli_replay_selection_publication_feedback_and_duplicate_replay(self):
        decisions=self.root/'decisions.json'; decisions.write_text(json.dumps({'*':{'decision':'eligible','score':.95,'tags':['test']}}))
        result=self.command('--decisions',str(decisions)); self.assertEqual(result.returncode,0,result.stderr)
        report=json.loads(result.stdout); self.assertTrue(report['simulation']); self.assertEqual(report['publications'],[{'group_id':'900201','nodes':1}])
        self.assertEqual(report['state']['feedback'],1)
        repeated=self.command('--decisions',str(decisions)); self.assertEqual(repeated.returncode,0,repeated.stderr)
        second=json.loads(repeated.stdout); self.assertEqual(second['duplicates'],3); self.assertFalse(second['publications'])
        self.assertEqual(second['state'],report['state'])
        self.assertFalse((self.root/'data/development').exists())
    def test_missing_simulated_verdict_prevents_publication(self):
        result=self.command(); self.assertEqual(result.returncode,0,result.stderr)
        self.assertFalse(json.loads(result.stdout)['publications'])
    def test_database_scope_rejects_live_profile_and_symlink(self):
        result=self.command('--database','data/development/state.sqlite3'); self.assertEqual(result.returncode,2)
        self.assertFalse((self.root/'data/development').exists())
        outside=self.root/'outside'; outside.mkdir(); (self.root/'data').mkdir(exist_ok=True)
        (self.root/'data/replay').symlink_to(outside,target_is_directory=True)
        result=self.command(); self.assertEqual(result.returncode,2)
        self.assertFalse(list(outside.iterdir()))
