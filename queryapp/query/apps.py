from django.apps import AppConfig



class QueryConfig(AppConfig):
    name = 'query'
    def ready(self):
      print("sleeping")
      #import time
      #time.sleep(2)  
      # importing model classes
