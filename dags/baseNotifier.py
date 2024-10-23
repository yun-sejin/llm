from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Sequence

from airflow.template.templater import Templater
from airflow.utils.context import context_merge

if TYPE_CHECKING:
    import jinja2

    from airflow import DAG
    from airflow.utils.context import Context


class BaseNotifier(Templater):
    """BaseNotifier class for sending notifications."""

    template_fields: Sequence[str] = ()
    template_ext: Sequence[str] = ()

    def __init__(self):
        pass

    @abstractmethod
    def notify(self, context: Context):
        """Send notification."""
        raise NotImplementedError("Subclasses should implement this method.")