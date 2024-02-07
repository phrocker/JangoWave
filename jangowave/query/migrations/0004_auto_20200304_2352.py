# Generated by Django 2.2.10 on 2020-03-04 23:52

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('query', '0003_accumulocluster_edgequery_fileupload_ingestconfiguration_result_scanresult'),
    ]

    operations = [
        migrations.AddField(
            model_name='edgequery',
            name='parent_query_id',
            field=models.CharField(default='', max_length=400),
        ),
        migrations.AlterField(
            model_name='result',
            name='cf',
            field=models.CharField(default='', max_length=2550),
        ),
        migrations.AlterField(
            model_name='result',
            name='cq',
            field=models.CharField(default='', max_length=2550),
        ),
        migrations.AlterField(
            model_name='result',
            name='row',
            field=models.CharField(default='', max_length=2550),
        ),
        migrations.AlterField(
            model_name='result',
            name='value',
            field=models.CharField(default='', max_length=2550),
        ),
        migrations.AlterField(
            model_name='scanresult',
            name='is_finished',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='scanresult',
            name='user',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL),
        ),
    ]