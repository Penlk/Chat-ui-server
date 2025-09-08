import json
import jwt
from django.test import TestCase
from django.utils.dateparse import parse_datetime
import time
from rest_framework.test import APIClient

from chat.models import Conversation, Project, Message


def build_x_user_data(sub: str) -> str:
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


class ProjectApiTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.sub = "user-a"
        self.headers = {"HTTP_X_USER_DATA": build_x_user_data(self.sub)}

    # List / Create
    def test_list_empty_then_create_201_and_sequence(self):
        # list empty
        resp = self.client.get("/api/chat/projects/", **self.headers)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), [])

        # create #1
        data1 = {"name": "P1", "description": "d1"}
        r1 = self.client.post("/api/chat/projects/", data=data1, format="json", **self.headers)
        self.assertEqual(r1.status_code, 201, r1.content)
        self.assertEqual(r1.json().get("project_id"), 1)

        # create #2 (sequence by sub)
        data2 = {"name": "P2", "description": "d2"}
        r2 = self.client.post("/api/chat/projects/", data=data2, format="json", **self.headers)
        self.assertEqual(r2.status_code, 201, r2.content)
        self.assertEqual(r2.json().get("project_id"), 2)

        # list non-empty
        rl = self.client.get("/api/chat/projects/", **self.headers)
        self.assertEqual(rl.status_code, 200)
        self.assertEqual(len(rl.json()), 2)

    def test_create_401_without_auth(self):
        resp = self.client.post("/api/chat/projects/", data={"name": "X"}, format="json")
        self.assertEqual(resp.status_code, 401)

    def test_create_400_missing_name(self):
        resp = self.client.post("/api/chat/projects/", data={}, format="json", **self.headers)
        self.assertEqual(resp.status_code, 400)

    def test_create_ignores_client_project_id(self):
        r1 = self.client.post("/api/chat/projects/", data={"name": "P1", "project_id": 99}, format="json", **self.headers)
        self.assertEqual(r1.status_code, 201)
        self.assertEqual(r1.json().get("project_id"), 1)

    # Retrieve
    def test_retrieve_200_and_404(self):
        # create
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        self.assertEqual(r.status_code, 201)
        pid = r.json()["project_id"]

        # ok
        g = self.client.get(f"/api/chat/projects/{pid}/", **self.headers)
        self.assertEqual(g.status_code, 200, g.content)
        self.assertEqual(g.json()["project_id"], pid)

        # 404 another pid
        g404 = self.client.get("/api/chat/projects/999/", **self.headers)
        self.assertEqual(g404.status_code, 404)

    def test_retrieve_404_other_user_same_pid(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        headers_b = {"HTTP_X_USER_DATA": build_x_user_data("user-b")}
        g = self.client.get(f"/api/chat/projects/{pid}/", **headers_b)
        self.assertEqual(g.status_code, 404)

    # Update / Partial
    def test_update_200_and_partial(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1", "description": "d1"}, format="json", **self.headers)
        self.assertEqual(r.status_code, 201)
        body_create = r.json()
        pid = body_create["project_id"]
        initial_updated_at = parse_datetime(body_create["updated_at"]) if body_create.get("updated_at") else None

        # full update
        # Небольшая пауза, чтобы updated_at точно отличался во времени
        time.sleep(0.01)
        u = self.client.put(f"/api/chat/projects/{pid}/", data={"name": "P1-new", "description": "d2", "is_active": False}, format="json", **self.headers)
        self.assertEqual(u.status_code, 200, u.content)
        put_body = u.json()
        self.assertEqual(put_body["name"], "P1-new")
        self.assertEqual(put_body["is_active"], False)
        if initial_updated_at and put_body.get("updated_at"):
            self.assertNotEqual(parse_datetime(put_body["updated_at"]), initial_updated_at)

        # partial
        time.sleep(0.01)
        p = self.client.patch(f"/api/chat/projects/{pid}/", data={"description": "d3"}, format="json", **self.headers)
        self.assertEqual(p.status_code, 200)
        patch_body = p.json()
        self.assertEqual(patch_body["description"], "d3")
        if put_body.get("updated_at") and patch_body.get("updated_at"):
            self.assertNotEqual(parse_datetime(patch_body["updated_at"]), parse_datetime(put_body["updated_at"]))

    def test_update_updates_updated_at(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P0"}, format="json", **self.headers)
        self.assertEqual(r.status_code, 201)
        body = r.json()
        pid = body["project_id"]
        initial_updated_at = parse_datetime(body["updated_at"]) if body.get("updated_at") else None
        time.sleep(0.01)
        u = self.client.patch(f"/api/chat/projects/{pid}/", data={"name": "P0x"}, format="json", **self.headers)
        self.assertEqual(u.status_code, 200)
        if initial_updated_at and u.json().get("updated_at"):
            self.assertNotEqual(parse_datetime(u.json()["updated_at"]), initial_updated_at)

    def test_update_401_without_auth(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        u = self.client.put(f"/api/chat/projects/{pid}/", data={"name": "x"}, format="json")
        self.assertEqual(u.status_code, 401)

    # Delete
    def test_delete_204_and_404(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        self.assertEqual(r.status_code, 201)
        pid = r.json()["project_id"]

        d = self.client.delete(f"/api/chat/projects/{pid}/", **self.headers)
        self.assertEqual(d.status_code, 204, d.content)

        d404 = self.client.delete(f"/api/chat/projects/{pid}/", **self.headers)
        self.assertEqual(d404.status_code, 404)

    def test_delete_401_without_auth(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        d = self.client.delete(f"/api/chat/projects/{pid}/")
        self.assertEqual(d.status_code, 401)

    # Isolation by sub
    def test_isolation_by_user_sub(self):
        # create two projects for user-a
        self.client.post("/api/chat/projects/", data={"name": "A1"}, format="json", **self.headers)
        self.client.post("/api/chat/projects/", data={"name": "A2"}, format="json", **self.headers)

        # other user-b should not see user-a projects
        headers_b = {"HTTP_X_USER_DATA": build_x_user_data("user-b")}
        resp_b = self.client.get("/api/chat/projects/", **headers_b)
        self.assertEqual(resp_b.status_code, 200)
        self.assertEqual(resp_b.json(), [])

    # Conversations endpoints
    def test_get_conversations_empty(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        self.assertEqual(r.status_code, 201)
        pid = r.json()["project_id"]

        g = self.client.get(f"/api/chat/projects/{pid}/conversations/", **self.headers)
        self.assertEqual(g.status_code, 200)
        self.assertEqual(g.json(), [])

    def test_add_conversation_and_remove(self):
        # create project
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        self.assertEqual(r.status_code, 201)
        pid = r.json()["project_id"]

        # create conversation for same user
        conv = Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=1, topic="T1")

        # add
        a = self.client.post(f"/api/chat/projects/{pid}/add-conversation/", data={"conversation_id": conv.conversation_id}, format="json", **self.headers)
        self.assertEqual(a.status_code, 200, a.content)

        # list conversations
        g = self.client.get(f"/api/chat/projects/{pid}/conversations/", **self.headers)
        self.assertEqual(g.status_code, 200)
        self.assertEqual(len(g.json()), 1)

        # remove
        rem = self.client.delete(f"/api/chat/projects/{pid}/remove-conversation/", data={"conversation_id": conv.conversation_id}, format="json", **self.headers)
        self.assertEqual(rem.status_code, 200, rem.content)

        # verify empty again
        g2 = self.client.get(f"/api/chat/projects/{pid}/conversations/", **self.headers)
        self.assertEqual(g2.status_code, 200)
        self.assertEqual(g2.json(), [])

    def test_add_conversation_400_missing_id(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        a = self.client.post(f"/api/chat/projects/{pid}/add-conversation/", data={}, format="json", **self.headers)
        self.assertEqual(a.status_code, 400)

    def test_add_conversation_404_not_found(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        a = self.client.post(f"/api/chat/projects/{pid}/add-conversation/", data={"conversation_id": 999}, format="json", **self.headers)
        self.assertEqual(a.status_code, 404)

    def test_add_conversation_404_other_user_conversation(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        Conversation.objects.create(sub="user-b", org_id=None, conversation_id=1, topic="T-b")
        a = self.client.post(f"/api/chat/projects/{pid}/add-conversation/", data={"conversation_id": 1}, format="json", **self.headers)
        self.assertEqual(a.status_code, 404)

    def test_move_conversation_between_projects(self):
        # two projects
        r1 = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        r2 = self.client.post("/api/chat/projects/", data={"name": "P2"}, format="json", **self.headers)
        p1, p2 = r1.json()["project_id"], r2.json()["project_id"]

        # conversation belongs to user
        conv = Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=1, topic="T1")

        # add to P1
        self.client.post(f"/api/chat/projects/{p1}/add-conversation/", data={"conversation_id": 1}, format="json", **self.headers)

        # move to P2
        mv = self.client.post(f"/api/chat/projects/{p2}/move-conversation/", data={"conversation_id": 1, "target_project_id": p2}, format="json", **self.headers)
        self.assertEqual(mv.status_code, 200, mv.content)
        self.assertEqual(mv.json()["new_project_id"], p2)

    def test_move_conversation_unassign_with_none(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        conv = Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=1, topic="T1")
        self.client.post(f"/api/chat/projects/{pid}/add-conversation/", data={"conversation_id": 1}, format="json", **self.headers)
        mv = self.client.post(f"/api/chat/projects/{pid}/move-conversation/", data={"conversation_id": 1, "target_project_id": None}, format="json", **self.headers)
        self.assertEqual(mv.status_code, 200)
        conv.refresh_from_db()
        self.assertIsNone(conv.project)

    def test_move_conversation_already_in_target(self):
        r1 = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        p1 = r1.json()["project_id"]
        conv = Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=1, topic="T1")
        self.client.post(f"/api/chat/projects/{p1}/add-conversation/", data={"conversation_id": 1}, format="json", **self.headers)
        mv = self.client.post(f"/api/chat/projects/{p1}/move-conversation/", data={"conversation_id": 1, "target_project_id": p1}, format="json", **self.headers)
        self.assertEqual(mv.status_code, 200)
        self.assertIn("already", mv.json().get("message", ""))

    def test_move_conversations_bulk(self):
        # two projects
        r1 = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        r2 = self.client.post("/api/chat/projects/", data={"name": "P2"}, format="json", **self.headers)
        p1, p2 = r1.json()["project_id"], r2.json()["project_id"]

        # create 3 conversations
        for cid in [1, 2, 3]:
            Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=cid, topic=f"T{cid}")

        # bulk move two to P2
        mv = self.client.post(f"/api/chat/projects/{p2}/move-conversations/", data={"conversation_ids": [1, 2], "target_project_id": p2}, format="json", **self.headers)
        self.assertEqual(mv.status_code, 200, mv.content)
        body = mv.json()
        self.assertEqual(body["moved_count"], 2)
        self.assertEqual(body["error_count"], 0)

        # verify
        g = self.client.get(f"/api/chat/projects/{p2}/conversations/", **self.headers)
        self.assertEqual(len(g.json()), 2)

    def test_move_conversations_bulk_unassign_to_none(self):
        # project and conversations assigned to it
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        for cid in [1, 2, 3]:
            Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=cid, topic=f"T{cid}")
            self.client.post(f"/api/chat/projects/{pid}/add-conversation/", data={"conversation_id": cid}, format="json", **self.headers)

        # unassign all to "external": target_project_id=None
        mv = self.client.post(
            f"/api/chat/projects/{pid}/move-conversations/",
            data={"conversation_ids": [1, 2, 3], "target_project_id": None},
            format="json",
            **self.headers,
        )
        self.assertEqual(mv.status_code, 200, mv.content)
        body = mv.json()
        self.assertEqual(body["moved_count"], 3)

        # verify all conversations have project unset
        for cid in [1, 2, 3]:
            conv = Conversation.objects.get(sub=self.sub, conversation_id=cid)
            self.assertIsNone(conv.project)

    def test_remove_conversation_not_in_project_404(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        conv = Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=1, topic="T1")
        # attempt to remove when not assigned
        rem = self.client.delete(
            f"/api/chat/projects/{pid}/remove-conversation/",
            data={"conversation_id": conv.conversation_id},
            format="json",
            **self.headers,
        )
        self.assertEqual(rem.status_code, 404)

    def test_move_conversations_400_missing_list(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        mv = self.client.post(f"/api/chat/projects/{pid}/move-conversations/", data={}, format="json", **self.headers)
        self.assertEqual(mv.status_code, 400)

    def test_move_conversations_400_not_list(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        mv = self.client.post(f"/api/chat/projects/{pid}/move-conversations/", data={"conversation_ids": "1"}, format="json", **self.headers)
        self.assertEqual(mv.status_code, 400)

    def test_move_conversations_404_target_not_found(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=1, topic="T1")
        mv = self.client.post(f"/api/chat/projects/{pid}/move-conversations/", data={"conversation_ids": [1], "target_project_id": 999}, format="json", **self.headers)
        self.assertEqual(mv.status_code, 404)

    def test_move_conversations_404_source_not_found(self):
        r = self.client.post("/api/chat/projects/", data={"name": "P"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=1, topic="T1")
        mv = self.client.post(f"/api/chat/projects/{pid}/move-conversations/", data={"conversation_ids": [1], "target_project_id": None, "source_project_id": 999}, format="json", **self.headers)
        self.assertEqual(mv.status_code, 404)

    def test_delete_removes_conversations_and_messages(self):
        # create project and conversation with messages
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        conv = Conversation.objects.create(sub=self.sub, org_id=None, conversation_id=1, topic="T1")
        conv.project = Project.objects.get(sub=self.sub, project_id=pid)
        conv.save()
        # messages under same conversation_id+sub
        Message.objects.create(sub=self.sub, conversation_id=1, message_id=1, message="u1", is_bot=False, message_type=0)
        Message.objects.create(sub=self.sub, conversation_id=1, message_id=2, message="b1", is_bot=True, message_type=0)
        # delete
        d = self.client.delete(f"/api/chat/projects/{pid}/", **self.headers)
        self.assertEqual(d.status_code, 204, d.content)
        self.assertFalse(Conversation.objects.filter(sub=self.sub, conversation_id=1).exists())
        self.assertEqual(Message.objects.filter(sub=self.sub, conversation_id=1).count(), 0)

    def test_project_id_sequence_separate_users(self):
        r1 = self.client.post("/api/chat/projects/", data={"name": "A"}, format="json", **self.headers)
        self.assertEqual(r1.json()["project_id"], 1)
        headers_b = {"HTTP_X_USER_DATA": build_x_user_data("user-b")}
        r2 = self.client.post("/api/chat/projects/", data={"name": "B"}, format="json", **headers_b)
        self.assertEqual(r2.json()["project_id"], 1)

    def test_get_conversations_401_without_auth(self):
        # create project under auth user
        r = self.client.post("/api/chat/projects/", data={"name": "P1"}, format="json", **self.headers)
        pid = r.json()["project_id"]
        g = self.client.get(f"/api/chat/projects/{pid}/conversations/")
        self.assertEqual(g.status_code, 401)
