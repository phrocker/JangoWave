# cities/models.py
from django.db import models


from django.db import models
import json
from django.conf import settings
from datetime import datetime

class Query(models.Model):
    name = models.CharField(max_length=2550)

    class Meta:
      verbose_name_plural = "queries"

    def __str__(self):
        return self.name
