import json
import jwt
from django.test import TestCase
from rest_framework.test import APIClient

from chat.models import Conversation, Message


def build_x_user_data(sub: str) -> str:
    # Генерируем валидный JWT (подпись проверяться не будет)
    token = jwt.encode({"sub": sub, "email": "test@example.com"}, "secret", algorithm="HS256")
    return json.dumps({
        "jwt_token": token,
        "user_data": {
            "email": "test@example.com",
            "full_name": "Test User",
            "orgs": [],
            "active_org_id": None,
        },
    })


class EditMessageApiTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.sub = "test-sub-123"
        self.auth_headers = {"HTTP_X_USER_DATA": build_x_user_data(self.sub)}

    def _create_conversation(self, conversation_id: int = 1) -> Conversation:
        return Conversation.objects.create(
            sub=self.sub,
            org_id=None,
            conversation_id=conversation_id,
            topic="Edit message tests",
        )

    def _create_message(self, conversation_id: int, message_id: int, text: str, is_bot: bool = False) -> Message:
        return Message.objects.create(
            sub=self.sub,
            conversation_id=conversation_id,
            message_id=message_id,
            message=text,
            is_bot=is_bot,
            message_type=0,
        )

    def test_edit_message_success_200(self):
        conv = self._create_conversation(conversation_id=1)
        self._create_message(1, 1, "msg-1", is_bot=False)
        self._create_message(1, 2, "msg-2", is_bot=False)
        self._create_message(1, 3, "msg-3", is_bot=False)

        url = f"/api/chat/messages/2/edit-message?conversationId=1"
        payload = {"message": "msg-2-edited"}
        resp = self.client.put(url, data=payload, format="json", **self.auth_headers)

        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body.get("edited"))
        self.assertGreaterEqual(body.get("deleted_following", 0), 1)

        # Проверим, что осталось только 2 сообщения и второе обновлено
        msgs = list(Message.objects.filter(sub=self.sub, conversation_id=1).order_by("message_id"))
        self.assertEqual(len(msgs), 2)
        self.assertEqual(msgs[1].message_id, 2)
        self.assertEqual(msgs[1].message, "msg-2-edited")

    def test_edit_message_401_no_auth(self):
        self._create_conversation(conversation_id=1)
        url = f"/api/chat/messages/1/edit-message?conversationId=1"
        resp = self.client.put(url, data={"message": "x"}, format="json")
        self.assertEqual(resp.status_code, 401, resp.content)

    def test_edit_message_400_no_conversationId(self):
        self._create_conversation(conversation_id=1)
        self._create_message(1, 1, "msg", is_bot=False)
        url = f"/api/chat/messages/1/edit-message"
        resp = self.client.put(url, data={"message": "edited"}, format="json", **self.auth_headers)
        self.assertEqual(resp.status_code, 400, resp.content)

    def test_edit_message_400_no_message_field(self):
        self._create_conversation(conversation_id=1)
        self._create_message(1, 1, "msg", is_bot=False)
        url = f"/api/chat/messages/1/edit-message?conversationId=1"
        resp = self.client.put(url, data={}, format="json", **self.auth_headers)
        self.assertEqual(resp.status_code, 400, resp.content)

    def test_edit_message_404_not_found(self):
        self._create_conversation(conversation_id=1)
        url = f"/api/chat/messages/999999/edit-message?conversationId=1"
        resp = self.client.put(url, data={"message": "x"}, format="json", **self.auth_headers)
        self.assertEqual(resp.status_code, 404, resp.content)

    def test_edit_message_400_bot_message_cannot_be_edited(self):
        self._create_conversation(conversation_id=1)
        self._create_message(1, 1, "bot-msg", is_bot=True)
        url = f"/api/chat/messages/1/edit-message?conversationId=1"
        resp = self.client.put(url, data={"message": "attempt"}, format="json", **self.auth_headers)
        self.assertEqual(resp.status_code, 400, resp.content)


