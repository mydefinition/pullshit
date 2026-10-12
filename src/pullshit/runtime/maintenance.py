"""Expire business content and reclaim its external storage."""
from __future__ import annotations
from datetime import datetime,timezone
import logging
from pathlib import Path
import time

class Maintenance:
    def __init__(self,settings,store,media=None,workers=None,*,clock=time.time):
        self.settings,self.store,self.media,self.workers,self.clock=settings,store,media,workers,clock
        self.last_cleanup=0.0; self.last_compaction_day=None

    def due(self):
        current=self.clock(); date=datetime.fromtimestamp(current,timezone.utc)
        physical=self.last_compaction_day!=date.date() and (date.hour>0 or date.minute>=30)
        return not self.last_cleanup or current-self.last_cleanup>=self.settings.data['retention']['cleanup_interval_seconds'] or physical

    def clean(self,compact=False):
        current=self.clock(); date=datetime.fromtimestamp(current,timezone.utc)
        physical=compact or self.last_compaction_day!=date.date() and (date.hour>0 or date.minute>=30)
        report=self.store.cleanup(now=current,compact=physical)
        if self.media is not None: report['media']=self.media.cleanup(now=current)
        if self.workers is not None: report['workers']=self.workers(self.settings.path('storage','worker_directory'),now=current)
        removed=0; bytes_removed=0
        directory=self.settings.path('logging','directory')
        if directory.exists():
            for path in directory.iterdir():
                if path.is_file() and not path.is_symlink() and path.stat().st_mtime<current-self.settings.data['retention']['log_days']*86400:
                    bytes_removed+=path.stat().st_size; path.unlink(); removed+=1
        report['logs']={'files_deleted':removed,'bytes_deleted':bytes_removed}
        self.last_cleanup=current
        if physical and not report.get('busy',False) and not report.get('wal_busy',False): self.last_compaction_day=date.date()
        logging.getLogger('pullshit').info('maintenance complete: %s',report)
        return report
