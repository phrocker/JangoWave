# cities/urls.py
from django.urls import path
from django.urls import path, include # new
from .views import MutateEventView, HomePageView, SearchResultsView, MetadataView, FieldMetadataView,MetadataChartView,MetadataEventCountsView

urlpatterns = [
    path('mutate/', MutateEventView.as_view(), name='mutate_page'),
    path('search/', SearchResultsView.as_view(), name='search_results'),
    path('fieldmetadata/', FieldMetadataView.as_view(), name='fieldmetadata'),
    path('chartdata', MetadataChartView.as_view(), name='chartdata'),
    path('eventcountdata', MetadataEventCountsView.as_view(), name='eventcountdata'),
    path('data/', MetadataView.as_view(), name='data'),
    path('accounts/', include('django.contrib.auth.urls')),
    #url(r'^login/$', auth_views.login, name='login'),
    path('', HomePageView.as_view(), name='home'),
]
