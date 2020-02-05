# cities/models.py
from django.db import models
import uuid

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

import os
def update_filename(instance, filename):
    ext = filename
    filename = "%s_%s" % (instance.uuid, ext)
    instance.originalfile = ext
    instance.filename = filename
    instance.status = "NEW"
    return os.path.join('files', filename)

class FileUpload(models.Model):
    filename = models.CharField(max_length=2550)
    originalfile = models.CharField(max_length=2550,default="")
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    status = models.CharField(max_length=20, default="NEW")
    document = models.FileField(null=True, blank=True,upload_to=update_filename)
    
    def __str__(self):
      return self.uuid + "." +  filename  


class Query(models.Model):
    name = models.CharField(max_length=2550)

    class Meta:
      verbose_name_plural = "queries"

    def __str__(self):
      return self.name
