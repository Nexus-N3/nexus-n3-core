"""Storage coordination for file output and session state."""

from nexus_n3.file_manager.FileManager import FileManager


class StorageOrchestrator:
    """Owns file manager lifecycle and session label handling."""

    def __init__(self, site: str, node_id: str = "standalone"):
        self.file_manager = FileManager(site, node_id=node_id)

    def set_session_label(self, label: str | None):
        if label is not None:
            self.file_manager.set_session_label(label)

    def set_file_path(self, path: str | None):
        if path:
            self.file_manager.set_base_path(path)
        else:
            self.file_manager.set_base_path(None)
