from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('chat', '0005_fix_conversation_id_foreign_key'),
    ]

    operations = [
        migrations.CreateModel(
            name='Project',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('sub', models.CharField(db_index=True, help_text='Уникальный идентификатор пользователя из JWT токена', max_length=36)),
                ('org_id', models.CharField(blank=True, db_index=True, help_text='Идентификатор организации', max_length=36, null=True)),
                ('project_id', models.IntegerField(default=0, help_text='Порядковый номер проекта для пользователя')),
                ('name', models.CharField(help_text='Название проекта', max_length=255)),
                ('description', models.TextField(blank=True, help_text='Описание проекта')),
                ('is_active', models.BooleanField(default=True, help_text='Активен ли проект')),
                ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'projects',
            },
        ),
        migrations.AddField(
            model_name='conversation',
            name='project',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='conversations', to='chat.project'),
        ),
    ]


