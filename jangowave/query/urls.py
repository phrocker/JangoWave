# cities/urls.py
from django.urls import path
from django.urls import path, include # new
from .views import MutateEventView, TravelSearchResultsView, TravelSearchView, FileUploadView, HomePageView,FileClearView, SearchResultsView, EdgeQueryResults,EdgeQueryView, MetadataView, FieldMetadataView,MetadataChartView,MetadataEventCountsView, FileStatusView,DeleteEventView

urlpatterns = [
    path('mutate/', MutateEventView.as_view(), name='mutate_page'),
    path('delete', DeleteEventView.as_view(), name='mutate_page'),
    path('search/', SearchResultsView.as_view(), name='search_results'),
    path('fieldmetadata/', FieldMetadataView.as_view(), name='fieldmetadata'),
    path('chartdata', MetadataChartView.as_view(), name='chartdata'),
    path('eventcountdata', MetadataEventCountsView.as_view(), name='eventcountdata'),
    path('data/', MetadataView.as_view(), name='data'),
    path('edge/', EdgeQueryView.as_view(), name='edge'),
    path('edge/results', EdgeQueryResults.as_view(), name='edgeresults'),
   # path('results/', QueryResult.as_view(), name='QueryResult'),
    path('files/', FileUploadView.as_view(), name='fileupload'),
    path('files/clear', FileClearView.as_view(), name='fileClear'),
    path('files/status', FileStatusView.as_view(), name='filestatus'),
    path('accounts/', include('django.contrib.auth.urls')),
    #url(r'^login/$', auth_views.login, name='login'),
    path('travel/', TravelSearchView.as_view(), name='home'),
    path('travel/search', TravelSearchResultsView.as_view(), name='home'),
    path('', HomePageView.as_view(), name='home'),
]
