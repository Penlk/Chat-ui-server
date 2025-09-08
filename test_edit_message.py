import json
import requests


BASE_URL = "http://localhost:8003/api/chat"


def make_x_user_data(sub: str = "eaf55af9-0467-44dc-8f13-8af689b97a09") -> str:
    # Невалидная подпись допустима: сервис читает payload без проверки подписи
    fake_jwt = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."  # header
        + json.dumps({"sub": sub, "email": "test@example.com"}).encode().hex()  # payload в hex, не важно
        + ".signature"
    )
    return json.dumps({
        "jwt_token": fake_jwt,
        "user_data": {
            "email": "test@example.com",
            "full_name": "Test User",
            "orgs": [],
            "active_org_id": None
        }
    })


def create_conversation(headers):
    url = f"{BASE_URL}/conversations/"
    data = {"topic": "Edit message tests"}
    resp = requests.post(url, headers=headers, json=data, timeout=30)
    assert resp.status_code in (200, 201), resp.text
    body = resp.json()
    # Возвращается Conversation с полем id (pk) и conversation_id (порядковый для пользователя)
    return body["id"], body.get("conversation_id", 0)


def create_message(headers, conversation_pk: int, text: str, is_bot: bool = False):
    url = f"{BASE_URL}/messages/"
    data = {
        "message": text,
        # В API messages POST ожидает поле conversation (pk), см. существующие примеры тестов
        "conversation": conversation_pk,
        "is_bot": is_bot,
        "message_type": 0
    }
    resp = requests.post(url, headers=headers, json=data, timeout=30)
    assert resp.status_code == 201, resp.text
    return resp.json()


def test_edit_message_success_200():
    headers = {
        "Content-Type": "application/json",
        "X-User-Data": make_x_user_data(),
    }

    conversation_pk, conversation_id = create_conversation(headers)

    # Создаем 3 сообщения пользователя
    m1 = create_message(headers, conversation_pk, "msg-1", is_bot=False)
    m2 = create_message(headers, conversation_pk, "msg-2", is_bot=False)
    m3 = create_message(headers, conversation_pk, "msg-3", is_bot=False)

    # Редактируем второе сообщение и ожидаем удаление последующих (m3)
    message_id = m2["message_id"]
    edit_url = f"{BASE_URL}/messages/{message_id}/edit-message?conversationId={conversation_id}"
    payload = {"message": "msg-2-edited"}
    resp = requests.put(edit_url, headers=headers, json=payload, timeout=30)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("edited") is True
    assert body.get("deleted_following") >= 1


def test_edit_message_401_no_auth():
    # Без X-User-Data, должен вернуть 401
    headers = {
        "Content-Type": "application/json",
    }

    # Даже если URL корректен, без аутентификации middleware оставит user_id пустым
    edit_url = f"{BASE_URL}/messages/1/edit-message?conversationId=1"
    payload = {"message": "new"}
    resp = requests.put(edit_url, headers=headers, json=payload, timeout=30)

    assert resp.status_code == 401, resp.text


def test_edit_message_400_no_conversationId():
    headers = {
        "Content-Type": "application/json",
        "X-User-Data": make_x_user_data(),
    }

    # Создадим разговор и одно сообщение
    conversation_pk, conversation_id = create_conversation(headers)
    m1 = create_message(headers, conversation_pk, "msg", is_bot=False)

    # Пробуем отредактировать без query-параметра conversationId
    message_id = m1["message_id"]
    edit_url = f"{BASE_URL}/messages/{message_id}/edit-message"  # без conversationId
    payload = {"message": "edited"}
    resp = requests.put(edit_url, headers=headers, json=payload, timeout=30)

    assert resp.status_code == 400, resp.text


def test_edit_message_400_no_message_field():
    headers = {
        "Content-Type": "application/json",
        "X-User-Data": make_x_user_data(),
    }

    conversation_pk, conversation_id = create_conversation(headers)
    m1 = create_message(headers, conversation_pk, "msg", is_bot=False)

    # Пустое тело без message
    message_id = m1["message_id"]
    edit_url = f"{BASE_URL}/messages/{message_id}/edit-message?conversationId={conversation_id}"
    resp = requests.put(edit_url, headers=headers, json={}, timeout=30)

    assert resp.status_code == 400, resp.text


def test_edit_message_404_not_found():
    headers = {
        "Content-Type": "application/json",
        "X-User-Data": make_x_user_data(),
    }

    # Создадим разговор, но не будем создавать сообщение с таким message_id
    _, conversation_id = create_conversation(headers)

    edit_url = f"{BASE_URL}/messages/999999/edit-message?conversationId={conversation_id}"
    payload = {"message": "will-fail"}
    resp = requests.put(edit_url, headers=headers, json=payload, timeout=30)

    assert resp.status_code == 404, resp.text


def test_edit_message_400_bot_message_cannot_be_edited():
    headers = {
        "Content-Type": "application/json",
        "X-User-Data": make_x_user_data(),
    }

    conversation_pk, conversation_id = create_conversation(headers)

    # Создаем бот-сообщение
    bot_msg = create_message(headers, conversation_pk, "bot-msg", is_bot=True)

    # Пытаемся отредактировать бот-сообщение
    message_id = bot_msg["message_id"]
    edit_url = f"{BASE_URL}/messages/{message_id}/edit-message?conversationId={conversation_id}"
    payload = {"message": "attempt-edit"}
    resp = requests.put(edit_url, headers=headers, json=payload, timeout=30)

    assert resp.status_code == 400, resp.text


