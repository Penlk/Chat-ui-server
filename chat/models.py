import logging
from django.db import models

logger = logging.getLogger(__name__)


class EmbeddingDocument(models.Model):
    """
    Модель документа для эмбеддинга, адаптированная под использование sub как уникального идентификатора пользователя.
    """
    id = models.AutoField(primary_key=True)
    sub = models.CharField(max_length=36, db_index=True, help_text="Уникальный идентификатор пользователя из JWT токена")
    org_id = models.CharField(max_length=36, null=True, blank=True, db_index=True, help_text="Идентификатор организации")
    document_id = models.IntegerField(default=0, help_text="Порядковый номер документа для пользователя")
    title = models.CharField(max_length=255, help_text="Заголовок документа")
    faiss_store = models.BinaryField(help_text="FAISS векторное хранилище")
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = 'embedding_documents'
        indexes = [
            models.Index(fields=['sub', 'created_at']),
            models.Index(fields=['sub', 'org_id']),
            models.Index(fields=['sub', 'document_id']),
        ]

    def __str__(self):
        return f"Document {self.id} - {self.title} (sub: {self.sub})"


class Conversation(models.Model):
    """
    Модель разговора, адаптированная под использование sub как уникального идентификатора пользователя.
    Теперь может быть связана с проектом.
    """
    id = models.AutoField(primary_key=True)
    sub = models.CharField(max_length=36, db_index=True, help_text="Уникальный идентификатор пользователя из JWT токена")
    org_id = models.CharField(max_length=36, null=True, blank=True, db_index=True, help_text="Идентификатор организации")
    conversation_id = models.IntegerField(default=0, help_text="Порядковый номер беседы для пользователя")
    topic = models.CharField(max_length=255, help_text="Тема разговора")
    project = models.ForeignKey('Project', on_delete=models.SET_NULL, null=True, blank=True, related_name='conversations', help_text="Проект, к которому принадлежит разговор")
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'conversations'
        indexes = [
            models.Index(fields=['sub', 'created_at']),
            models.Index(fields=['sub', 'org_id']),
            models.Index(fields=['sub', 'conversation_id']),
            models.Index(fields=['project', 'created_at']),  # Новый индекс для проектов
        ]

    def __str__(self):
        return f"Conversation {self.id} - {self.topic} (sub: {self.sub})"

class Message(models.Model):
    """
    Модель сообщения, адаптированная под использование sub как уникального идентификатора пользователя.
    """
    id = models.AutoField(primary_key=True)
    sub = models.CharField(max_length=36, db_index=True, help_text="Уникальный идентификатор пользователя из JWT токена")
    conversation_id = models.IntegerField(help_text="Порядковый номер беседы для пользователя")
    message_id = models.IntegerField(default=0, help_text="Порядковый номер сообщения для пользователя")
    message = models.TextField(help_text="Текст сообщения")
    is_bot = models.BooleanField(default=False, help_text="Сообщение от бота")
    message_type = models.IntegerField(default=0, help_text="Тип сообщения")
    embedding_message_doc = models.ForeignKey('EmbeddingDocument', on_delete=models.SET_NULL, null=True, blank=True)
    messages = models.JSONField(default=list, help_text="История сообщений")
    tokens = models.IntegerField(default=0, help_text="Количество токенов")
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = 'messages'
        indexes = [
            models.Index(fields=['sub', 'created_at']),
            models.Index(fields=['sub', 'conversation_id']),
            models.Index(fields=['conversation_id', 'created_at']),
            models.Index(fields=['sub', 'message_id']),
        ]

    def __str__(self):
        return f"Message {self.id} - {self.message[:50]}... (sub: {self.sub})"


class Prompt(models.Model):
    """
    Модель промпта, адаптированная под использование sub как уникального идентификатора пользователя.
    """
    id = models.AutoField(primary_key=True)
    sub = models.CharField(max_length=36, db_index=True, help_text="Уникальный идентификатор пользователя из JWT токена")
    prompt_id = models.IntegerField(default=0, help_text="Порядковый номер промпта для пользователя")
    title = models.CharField(max_length=255, help_text="Заголовок промпта")
    content = models.TextField(help_text="Содержание промпта")
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = 'prompts'
        indexes = [
            models.Index(fields=['sub', 'created_at']),
            models.Index(fields=['sub', 'prompt_id']),
        ]

    def __str__(self):
        return f"Prompt {self.id} - {self.title} (sub: {self.sub})"


class Setting(models.Model):
    """
    Модель настроек системы.
    """
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255, unique=True, help_text="Название настройки")
    value = models.TextField(help_text="Значение настройки")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'settings'

    def __str__(self):
        return f"Setting {self.name} = {self.value}"


class TokenUsage(models.Model):
    """
    Модель использования токенов, адаптированная под использование sub как уникальный идентификатор пользователя.
    Аналогично balance_transactions в Tarrification.
    """
    id = models.AutoField(primary_key=True)
    sub = models.CharField(max_length=36, unique=True, db_index=True, help_text="Уникальный идентификатор пользователя из JWT токена")
    tokens = models.BigIntegerField(default=0, help_text="Общее количество использованных токенов")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'token_usage'
        indexes = [
            models.Index(fields=['sub']),
        ]

    def __str__(self):
        return f"TokenUsage for {self.sub}: {self.tokens} tokens"

class Project(models.Model):
    """
    Модель проекта, который может содержать группу чатов (conversations).
    """
    id = models.AutoField(primary_key=True)
    sub = models.CharField(max_length=36, db_index=True, help_text="Уникальный идентификатор пользователя из JWT токена")
    org_id = models.CharField(max_length=36, null=True, blank=True, db_index=True, help_text="Идентификатор организации")
    project_id = models.IntegerField(default=0, help_text="Порядковый номер проекта для пользователя")
    name = models.CharField(max_length=255, help_text="Название проекта")
    description = models.TextField(blank=True, help_text="Описание проекта")
    is_active = models.BooleanField(default=True, help_text="Активен ли проект")
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'projects'
        indexes = [
            models.Index(fields=['sub', 'created_at']),
            models.Index(fields=['sub', 'org_id']),
            models.Index(fields=['sub', 'project_id']),
        ]

    def __str__(self):
        return f"Project {self.id} - {self.name} (sub: {self.sub})"
