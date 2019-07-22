# cities/urls.py
from django.urls import path
from django.urls import path, include # new
from .views import HomePageView, SearchResultsView

urlpatterns = [
    path('search/', SearchResultsView.as_view(), name='search_results'),
    path('accounts/', include('django.contrib.auth.urls')),
    #url(r'^login/$', auth_views.login, name='login'),
    path('', HomePageView.as_view(), name='home'),
]
