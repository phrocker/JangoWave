from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.shortcuts import render_to_response
from ctypes import cdll
from argparse import ArgumentParser
import heapq
from sortedcontainers import SortedList, SortedSet, SortedDict
import ctypes
import itertools
import os
import traceback
import sys
import Uid_pb2
import time
import json
from django.http import JsonResponse
from concurrent.futures import ThreadPoolExecutor
#from multiprocessing import Queue
import multiprocessing
# cities/views.py
from django.views.generic import TemplateView, ListView, View

from stronghold.views import StrongholdPublicMixin
import threading

from .models import Query
from .models import UserAuths
from .models import Auth
from  .WritableUtils import *
from .rangebuilder import *
from luqum.parser import lexer, parser, ParseError
from luqum.pretty import prettify
from luqum.utils import UnknownOperationResolver, LuceneTreeVisitorV2
from luqum.exceptions import OrAndAndOnSameLevel
from luqum.tree import OrOperation, AndOperation, UnknownOperation
from luqum.tree import Word  # noqa: F401
import asyncio
from collections import deque
import queue
import concurrent.futures

class UserQuery:
    def __init__(self,user,query_string,result_count, query_id):
        self.user=user
        self.query_string=query_string
        self.result_count=result_count
        self.query_id=query_id

    def get_user(self):
        return self.user

    def get_query_string(self):
        return self.query_string

    def get_result_count(self):
        return result_count

    def get_query_id(self):
        return query_id

class UsersQueries:
    def __init__(self,user):
        self.queries = list()

    def add_query(self, kwer : UserQuery):
        self.queries.append(kwer)

    def get_queries(self):
        return self.queries