from django import forms
from .models import FileUpload, AccumuloCluster
from django.forms import ModelForm, PasswordInput
class AccumuloClusterForm(forms.ModelForm):
    class Meta:
        model = AccumuloCluster
        exclude = ()
        widgets = {
            'password': PasswordInput(render_value = True),
        }

class DocumentForm(forms.ModelForm):
    class Meta:
        model = FileUpload
        fields = ( 'document',)
        #widgets = {'uuid': forms.HiddenInput()}
