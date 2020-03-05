from django.apps import AppConfig
from .celery import pouplateEventCountMetadata, populateFieldMetadata,populateMetadata

class QueryConfig(AppConfig):
    name = 'query'
    def ready(self):
      print("Starting Jangowave Query")
