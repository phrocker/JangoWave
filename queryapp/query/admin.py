# cities/admin.py
from django.contrib import admin

from .models import Query
from .models import UserAuths
from .models import Auth

class QueryAdmin(admin.ModelAdmin):
    list_display = ("name",)


class AuthAdmin(admin.ModelAdmin):
    list_display = ("auth",)



class AuthsAdmin(admin.ModelAdmin):
    list_display = ("name",)

admin.site.register(Query, QueryAdmin)
admin.site.register(Auth, AuthAdmin)
admin.site.register(UserAuths, AuthsAdmin)
