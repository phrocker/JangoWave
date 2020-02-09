from django.apps import AppConfig
from .celery import pouplateEventCountMetadata, populateFieldMetadata,populateMetadata

class QueryConfig(AppConfig):
    name = 'query'
    def ready(self):
      print("sleeping")
     # populateMetadata.delay() 
      print("get field")  
      #populateFieldMetadata.delay()
      #pouplateEventCountMetadata.delay()
      #import time
      #time.sleep(2)  
      # importing model classes
