from django.apps import AppConfig
from .celery import populateFieldMetadata, populateFieldMetadata,populateMetadata

class QueryConfig(AppConfig):
    name = 'query'
    def ready(self):
     # populateMetadata.delay() 
      #populateFieldMetadata.delay()
      populateFieldMetadata.delay()
      #import time
      #time.sleep(2)  
      # importing model classes
