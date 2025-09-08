from django.db import migrations


def rename_conversation_column(apps, schema_editor):
    vendor = getattr(schema_editor.connection, 'vendor', '')
    try:
        if vendor == 'sqlite':
            # Проверяем наличие колонок через PRAGMA
            with schema_editor.connection.cursor() as cursor:
                cursor.execute("PRAGMA table_info(messages);")
                cols = [row[1] for row in cursor.fetchall()]
                if 'conversation_id' in cols:
                    return
                if 'conversation' in cols:
                    cursor.execute("ALTER TABLE messages RENAME COLUMN conversation TO conversation_id;")
        elif vendor == 'postgresql':
            with schema_editor.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'messages' AND column_name = 'conversation_id'
                    );
                """)
                has_conversation_id = cursor.fetchone()[0]
                if has_conversation_id:
                    return
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'messages' AND column_name = 'conversation'
                    );
                """)
                has_conversation = cursor.fetchone()[0]
                if has_conversation:
                    cursor.execute("ALTER TABLE messages RENAME COLUMN conversation TO conversation_id;")
        else:
            # Для других СУБД пробуем универсально
            with schema_editor.connection.cursor() as cursor:
                try:
                    cursor.execute("ALTER TABLE messages RENAME COLUMN conversation TO conversation_id;")
                except Exception:
                    pass
    except Exception:
        # Мягко игнорируем, чтобы не ломать миграции в средах без этой колонки
        pass


class Migration(migrations.Migration):

    dependencies = [
        ('chat', '0006_add_project_and_fk'),
    ]

    operations = [
        migrations.RunPython(rename_conversation_column, reverse_code=migrations.RunPython.noop),
    ]


