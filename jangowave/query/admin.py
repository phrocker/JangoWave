# cities/admin.py
from django.contrib import admin

from .models import Query
from .models import UserAuths
from .models import Auth
from .models import AccumuloCluster
from .models import IngestConfiguration
from .models import DatawaveWebservers

class QueryAdmin(admin.ModelAdmin):
    list_display = ("name",)


class AuthAdmin(admin.ModelAdmin):
    list_display = ("auth",)


class AuthsAdmin(admin.ModelAdmin):
    list_display = ("name",)

class AccumuloClusterAdmin(admin.ModelAdmin):
    list_display = ("instance",)

class IngestAdmin(admin.ModelAdmin):
    list_display = ("name",)

class DatawaveWebserverAdmin(admin.ModelAdmin):
    list_display = ("url",)

admin.site.register(Query, QueryAdmin)
admin.site.register(Auth, AuthAdmin)
admin.site.register(UserAuths, AuthsAdmin)
admin.site.register(AccumuloCluster, AccumuloClusterAdmin)
admin.site.register(IngestConfiguration, IngestAdmin)
admin.site.register(DatawaveWebservers, DatawaveWebserverAdmin)
