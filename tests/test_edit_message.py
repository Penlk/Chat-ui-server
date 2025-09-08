import os
import json
import requests
import jwt
from unittest import skip
from django.test import TestCase, SimpleTestCase
from rest_framework.test import APIClient
from chat.models import Conversation, Message


BASE_URL = os.environ.get("TEST_BASE_URL", "http://localhost:8003/api/chat")


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
    edit_url = f"{BASE_URL}/messages/{message_id}/edit-message/?conversationId={conversation_id}"
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
    edit_url = f"{BASE_URL}/messages/1/edit-message/?conversationId=1"
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
    edit_url = f"{BASE_URL}/messages/{message_id}/edit-message/"  # без conversationId
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
    edit_url = f"{BASE_URL}/messages/{message_id}/edit-message/?conversationId={conversation_id}"
    resp = requests.put(edit_url, headers=headers, json={}, timeout=30)

    assert resp.status_code == 400, resp.text


def test_edit_message_404_not_found():
    headers = {
        "Content-Type": "application/json",
        "X-User-Data": make_x_user_data(),
    }

    # Создадим разговор, но не будем создавать сообщение с таким message_id
    _, conversation_id = create_conversation(headers)

    edit_url = f"{BASE_URL}/messages/999999/edit-message/?conversationId={conversation_id}"
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
    edit_url = f"{BASE_URL}/messages/{message_id}/edit-message/?conversationId={conversation_id}"
    payload = {"message": "attempt-edit"}
    resp = requests.put(edit_url, headers=headers, json=payload, timeout=30)

    assert resp.status_code == 400, resp.text


# Обёртка для Django test runner
# @skip("HTTP тесты отключены: используйте APIClient-тесты ниже без поднятого сервера")
# class EditMessageHttpTests(SimpleTestCase):
#     def test_edit_message_success_200(self):
#         test_edit_message_success_200()

#     def test_edit_message_401_no_auth(self):
#         test_edit_message_401_no_auth()

#     def test_edit_message_400_no_conversationId(self):
#         test_edit_message_400_no_conversationId()

#     def test_edit_message_400_no_message_field(self):
#         test_edit_message_400_no_message_field()

#     def test_edit_message_404_not_found(self):
#         test_edit_message_404_not_found()

#     def test_edit_message_400_bot_message_cannot_be_edited(self):
#         test_edit_message_400_bot_message_cannot_be_edited()


class EditMessageTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.sub = "test-sub-123"
        token = jwt.encode({"sub": self.sub, "email": "test@example.com"}, "secret", algorithm="HS256")
        self.headers = {"HTTP_X_USER_DATA": json.dumps({
            "jwt_token": token,
            "user_data": {"email": "test@example.com", "full_name": "Test User", "orgs": [], "active_org_id": None}
        })}
        self.conv = Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=1, topic="Edit message tests")

    def _create_user(self, mid: int, text: str) -> Message:
        return Message.objects.create(sub=self.sub, conversation_id=1, message_id=mid, message=text, is_bot=False, message_type=0)

    def _create_bot(self, mid: int, text: str) -> Message:
        return Message.objects.create(sub=self.sub, conversation_id=1, message_id=mid, message=text, is_bot=True, message_type=0)

    def test_edit_first_of_ten_deletes_9_user_and_10_bot(self):
        for i in range(10):
            self._create_user(2 * i + 1, f"u-{i+1}")
            self._create_bot(2 * i + 2, f"b-{i+1}")
        url = "/api/chat/messages/1/edit-message/?conversationId=1"
        resp = self.client.put(url, data={"message": "u-1-edited"}, format="json", **self.headers)
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(resp.json().get("deleted_following"), 19)
        msgs = list(Message.objects.filter(sub=self.sub, conversation_id=1).order_by("message_id"))
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0].message_id, 1)
        self.assertFalse(msgs[0].is_bot)
        self.assertEqual(msgs[0].message, "u-1-edited")

    def test_edit_single_user_message_deletes_only_bot(self):
        self._create_user(1, "u-1")
        self._create_bot(2, "b-1")
        url = "/api/chat/messages/1/edit-message/?conversationId=1"
        resp = self.client.put(url, data={"message": "u-1-edited"}, format="json", **self.headers)
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(resp.json().get("deleted_following"), 1)
        msgs = list(Message.objects.filter(sub=self.sub, conversation_id=1).order_by("message_id"))
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0].message_id, 1)
        self.assertFalse(msgs[0].is_bot)
        self.assertEqual(msgs[0].message, "u-1-edited")

    def test_edit_tenth_of_ten_deletes_only_last_bot(self):
        for i in range(10):
            self._create_user(2 * i + 1, f"u-{i+1}")
            self._create_bot(2 * i + 2, f"b-{i+1}")
        url = "/api/chat/messages/19/edit-message/?conversationId=1"
        resp = self.client.put(url, data={"message": "u-10-edited"}, format="json", **self.headers)
        self.assertEqual(resp.status_code, 200, resp.content)
        self.assertEqual(resp.json().get("deleted_following"), 1)
        msgs = list(Message.objects.filter(sub=self.sub, conversation_id=1).order_by("message_id"))
        self.assertEqual(len(msgs), 19)
        self.assertEqual(msgs[-1].message_id, 19)
        self.assertFalse(msgs[-1].is_bot)
        self.assertEqual(msgs[-1].message, "u-10-edited")

    def test_success_edit_middle_message_removes_following(self):
        self._create_user(1, "msg-1")
        self._create_user(2, "msg-2")
        self._create_user(3, "msg-3")
        url = "/api/chat/messages/2/edit-message/?conversationId=1"
        resp = self.client.put(url, data={"message": "msg-2-edited"}, format="json", **self.headers)
        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body.get("edited"))
        self.assertGreaterEqual(body.get("deleted_following", 0), 1)
        msgs = list(Message.objects.filter(sub=self.sub, conversation_id=1).order_by("message_id"))
        self.assertEqual(len(msgs), 2)
        self.assertEqual(msgs[1].message_id, 2)
        self.assertEqual(msgs[1].message, "msg-2-edited")

    def test_401_no_auth(self):
        url = "/api/chat/messages/1/edit-message/?conversationId=1"
        resp = self.client.put(url, data={"message": "x"}, format="json")
        self.assertEqual(resp.status_code, 401, resp.content)

    def test_400_no_conversationId(self):
        self._create_user(1, "msg")
        url = "/api/chat/messages/1/edit-message/"  # без conversationId
        resp = self.client.put(url, data={"message": "edited"}, format="json", **self.headers)
        self.assertEqual(resp.status_code, 400, resp.content)

    def test_400_no_message_field(self):
        self._create_user(1, "msg")
        url = "/api/chat/messages/1/edit-message/?conversationId=1"
        resp = self.client.put(url, data={}, format="json", **self.headers)
        self.assertEqual(resp.status_code, 400, resp.content)

    def test_404_not_found(self):
        url = "/api/chat/messages/999999/edit-message/?conversationId=1"
        resp = self.client.put(url, data={"message": "x"}, format="json", **self.headers)
        self.assertEqual(resp.status_code, 404, resp.content)

    def test_400_bot_message_cannot_be_edited(self):
        self._create_bot(1, "bot-msg")
        url = "/api/chat/messages/1/edit-message/?conversationId=1"
        resp = self.client.put(url, data={"message": "attempt"}, format="json", **self.headers)
        self.assertEqual(resp.status_code, 400, resp.content)
