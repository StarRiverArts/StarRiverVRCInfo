from .entry import EntryTab
from .search_results import SearchResultsTab
from .taiwan import TaiwanTab
from .user import UserTab
from .history import HistoryTab
from .settings import SettingsTab
from .about import AboutTab
from .log import LogTab

# Legacy aliases kept so any remaining test imports don't break
from .search_results import SearchResultsTab as FilterTab
from .search_results import SearchResultsTab as DataTab
from .search_results import SearchResultsTab as ListTab

__all__ = [
    "EntryTab",
    "SearchResultsTab",
    "TaiwanTab",
    "UserTab",
    "HistoryTab",
    "SettingsTab",
    "AboutTab",
    "LogTab",
    # legacy
    "FilterTab",
    "DataTab",
    "ListTab",
]
