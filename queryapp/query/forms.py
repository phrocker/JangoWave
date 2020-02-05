from django import forms
from .models import FileUpload

class DocumentForm(forms.ModelForm):
    class Meta:
        model = FileUpload
        fields = ( 'document',)
        #widgets = {'uuid': forms.HiddenInput()}
