# cities/admin.py
from django.contrib import admin

from .models import Query

class QueryAdmin(admin.ModelAdmin):
    list_display = ("name",)

admin.site.register(Query, QueryAdmin)
