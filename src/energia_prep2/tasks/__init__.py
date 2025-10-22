"""
Podpakiet energia_prep2.tasks — kolejność wykonywania (UTC-only, bez widoków 30_views (depreciated)):
bootstrap → date_dim → csv_import → tge_fetch → finalize

Uwaga:
- 'views_triggers' pozostaje w repo, ale NIE jest uruchamiany w pipeline.
"""
__all__ = []