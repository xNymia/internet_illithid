from ..models import UserProfile
from django.shortcuts import render, render_to_response
from django.template.context import RequestContext


def user_details(details, response, user=None, *args, **kwargs):
    if user:
        if kwargs['is_new']:
            attrs = {'user': user}
            UserProfile.objects.create(**attrs)