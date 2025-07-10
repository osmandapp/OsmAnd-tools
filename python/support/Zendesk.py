import base64
import json
import os
from typing import Any

import requests

tickets_view = os.getenv('TICKETS', 'new')

auto_reply_prompt = os.getenv('AUTOREPLY_PROMPT', 'AUTOREPLY_EMPTY_INSTRUCTION')
auto_reply_internal_note = os.getenv('AUTOREPLY_INTERNAL', 'false').lower() == 'true'
auto_reply_category = [cat.replace('$$', '$$$')
                       for cat in os.getenv('AUTOREPLY_CATEGORY', '').split(',') if cat]

domain = os.getenv('ZENDESK_DOMAIN', 'osmandhelp')
view = os.getenv('ZENDESK_VIEW', '150931385')

user = os.getenv('ZENDESK_USER', 'osmand.help@gmail.com')
token = os.getenv('ZENDESK_TOKEN')
encoded_auth = base64.b64encode(f"{user}/token:{token}".encode()).decode()

if not all([token]):
    raise ValueError("Missing required environment variables (ZENDESK_TOKEN)")

# A Zendesk client to fetch tickets, load comments and send auto‐replies.
class Zendesk:
    def __init__(self, prompts):
        self.prompts = prompts

    @staticmethod
    def get_tickets(next_cursor: str) -> dict:
        url = f"https://{domain}.zendesk.com/api/v2/views/{view}/tickets.json?page[size]=1000"
        if next_cursor:
            url = next_cursor
        headers = {"Authorization": f"Basic {encoded_auth}"}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching tickets: {response.status_code}")
            print(response.content)
            exit(1)
        return response.json()

    @staticmethod
    def _get_ticket_comments(ticket_id: str):
        url = f"https://{domain}.zendesk.com/api/v2/tickets/{ticket_id}/comments.json"
        headers = {"Authorization": f"Basic {encoded_auth}", "Content-Type": "application/json"}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to fetch comments for ticket ID {ticket_id}. Status code: {response.status_code}")
            return []
        comments = response.json().get('comments', [])
        return comments

    def get_answer_and_question(self, tiket: dict) -> (str, str):
        zend_comments = self._get_ticket_comments(tiket['id'])
        support_comments = []
        requester_comments = []
        for c in zend_comments:
            if 'name' not in c['via']['source']['from'] or c['via']['source']['from']['name'] == 'OsmAnd':
                support_comments.append(c['body'])
            else:
                requester_comments.append(c['body'])

        description = tiket['description']
        return (max(support_comments, key=len),
                max(requester_comments, key=len) if len(description) < 32 and len(
                    requester_comments) > 1 else description)

    def load_answer(self, ticket_data: dict) -> str:
        ticket_id = ticket_data['id']
        comments = self._get_ticket_comments(ticket_id)
        if len(comments) > 1:
            return comments[1]['body']
        return ''

    def auto_reply(self, ticket: dict) -> Any:
        if not (auto_reply_prompt and len(auto_reply_category) > 0 and auto_reply_category[0] and
                ticket['category'] in auto_reply_category and tickets_view == "new"):
            return

        ticket_id = ticket['id']
        print(f"Auto-replying ticket# {ticket_id}, internal reply: {auto_reply_internal_note}")

        url = f"https://{domain}.zendesk.com/api/v2/tickets/{ticket_id}.json"
        headers = {"Authorization": f"Basic {encoded_auth}", "Content-Type": "application/json"}

        comment_body = self.prompts[auto_reply_prompt].format(answer=ticket['answer'])
        data = {
            "ticket": {
                "comment": {
                    "body": comment_body,
                    "public": not auto_reply_internal_note
                }
            }
        }

        if not auto_reply_internal_note:
            data["ticket"]["status"] = "pending"
        try:
            response = requests.put(url, headers=headers, data=json.dumps(data))
            response.raise_for_status()
            print(f"Auto-reply sent successfully for ticket: {ticket_id}")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error sending auto-reply for ticket: {ticket_id}: {e}")
            return None
