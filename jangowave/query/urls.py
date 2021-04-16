# cities/urls.py
from django.urls import path
from django.urls import path, include # new
from .views import MutateEventView, FileUploadView, HomePageView, SearchResultsView, EdgeQueryResults,EdgeQueryView, MetadataView, FieldMetadataView,MetadataChartView,MetadataEventCountsView, FileStatusView,DeleteEventView, FieldExpansionView

urlpatterns = [
    path('mutate/', MutateEventView.as_view(), name='mutate_page'),
    path('delete', DeleteEventView.as_view(), name='mutate_page'),
    path('search/', SearchResultsView.as_view(), name='search_results'),
    path('fieldmetadata/', FieldMetadataView.as_view(), name='fieldmetadata'),
    path('fieldmetadata/field', FieldExpansionView.as_view(), name='fieldmetadata'),
    path('chartdata', MetadataChartView.as_view(), name='chartdata'),
    path('eventcountdata', MetadataEventCountsView.as_view(), name='eventcountdata'),
    path('data/', MetadataView.as_view(), name='data'),
    path('edge/', EdgeQueryView.as_view(), name='edge'),
    path('edge/results', EdgeQueryResults.as_view(), name='edgeresults'),
   # path('results/', QueryResult.as_view(), name='QueryResult'),
    path('files/', FileUploadView.as_view(), name='fileupload'),
    path('files/status', FileStatusView.as_view(), name='filestatus'),
    path('accounts/', include('django.contrib.auth.urls')),
    #url(r'^login/$', auth_views.login, name='login'),
    path('', HomePageView.as_view(), name='home'),
]
