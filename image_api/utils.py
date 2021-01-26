from django.shortcuts import render
from rest_framework.views import APIView
from django.http import HttpResponse, HttpRequest
# Create your views here.

from django.http import JsonResponse
import time

def json_response(data, code=200, **extra):
    data = {"code": code, "msg": "success", "data": data,}
    for k, v in extra.items():
        data[k] = v
    return JsonResponse(data)

def json_error(error_string="", code=500, **kwargs):
    data = {"code": code, "msg": error_string, "data": {}}
    data.update(kwargs)
    return JsonResponse(data)


def timetamp_formatter(t):
    named_tuple = time.localtime(t)
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", named_tuple)
    return time_string