from django.shortcuts import render, render_to_response
from django.template.context import RequestContext


def index(request):
    context = RequestContext(request, {'test': 'test data 123'})
    return render_to_response('pages/index.html', context_instance=context)

