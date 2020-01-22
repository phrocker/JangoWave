# cities/models.py
from django.db import models


from django.db import models
import json
from django.conf import settings
from datetime import datetime
from django.contrib.auth.models import User
class Auth(models.Model):
    auth = models.CharField(max_length=64)

    def __str__(self):
      return self.auth
class UserAuths(models.Model):
    ## should probably define  custom user model
    name = models.ForeignKey(User, on_delete=models.CASCADE)
    authorizations = models.ManyToManyField(Auth)
  


class Query(models.Model):
    name = models.CharField(max_length=2550)

    class Meta:
      verbose_name_plural = "queries"

    def __str__(self):
      return self.name
