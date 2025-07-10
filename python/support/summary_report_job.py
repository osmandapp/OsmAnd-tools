import datetime
import json
import os
from pathlib import Path

import yaml

import Knowledge
import LLM
import Zendesk

knowedge_id = os.getenv('KNOWLEDGE', 'none')


class Report:
    def __init__(self):
        script_dir = Path(__file__).parent
        with open(script_dir / 'prompts.yaml', 'r') as f:
            prompts = yaml.safe_load(f)

        self.prompts = prompts

        # LLM provider params
        self.action_original_answer = os.getenv('ORIGINAL_ANSWER', '')
        self.is_exclude_processed = os.getenv('EXCLUDE_PROCESSED', 'false').lower() == 'true'

        self.limit = int(os.getenv('LIMIT', 1))
        self.ticket_tags = [tag for tag in os.getenv('TICKET_TAGS', '').split(',') if tag]

        self.zendesk = Zendesk.Zendesk(prompts)
        self.knowledge = Knowledge.Knowledge(prompts)
        self.llm = LLM.LLM(prompts)

        self.processed_tickets = []
        if self.is_exclude_processed:
            try:
                with open("runs.json", "r") as f:
                    runs_data = json.load(f)
                    for run in runs_data:
                        if run["model"] == LLM.MODEL_NAME and run["knowledge"] == knowedge_id:
                            self.processed_tickets.extend(run["ticketIds"])
            except FileNotFoundError:
                pass
        self.categories = {}
        self.all_tickets = []

    def run(self):
        start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        current_time = datetime.datetime.now().strftime("%H_%M")
        print(f"Start processing: Model={LLM.MODEL_FULL_NAME}, Date={current_date}", flush=True)

        limit = 0
        proc_ind = 0
        data = []
        after_cursor = ''
        while limit < self.limit:
            if proc_ind >= len(data):
                if after_cursor is None:
                    break
                data_response = self.zendesk.get_tickets(after_cursor)
                after_cursor = data_response.get('links', {}).get('next', None)
                data = sorted(data_response['tickets'], key=lambda x: x['id'], reverse=True)
                proc_ind = 0
                if not data:
                    break
            zend_ticket = data[proc_ind]
            proc_ind += 1

            if self.ticket_tags and not set(zend_ticket['tags']).intersection(set(self.ticket_tags)):
                continue
            if self.is_exclude_processed and zend_ticket['id'] in self.processed_tickets:
                print(f"Skip ticket {zend_ticket['id']} - already processed")
                continue
            limit += 1
            subject = zend_ticket['subject']
            description = zend_ticket['description']

            answer = ''
            if self.action_original_answer == 'review' or self.action_original_answer == 'load':
                answer = self.zendesk.load_answer(zend_ticket)
            if self.action_original_answer == 'review':
                template = self.prompts["ANALYZE_TICKET_INSTRUCTION"]
            else:
                template = self.prompts["NEW_TICKET_INSTRUCTION"]

            context = self.knowledge.knowledge_context(subject, description, answer)
            prompt = template.format(
                subject=subject, description=description, answer=answer,
                ticket_structure=self.prompts["TICKET_STRUCTURE"],
                category_definition=self.prompts["CATEGORY_DEFINITION"],
                context=context,
            )

            print(f"Ticket #: {zend_ticket['id']}. Knowledge type: {Knowledge.knowledge_type()}. Asking LLM ...")
            ticket_response = self.llm.ask(prompt)

            if ticket_response is not None:
                ticket_response['id'] = zend_ticket['id']
                ticket_response['url'] = f"https://{Zendesk.domain}.zendesk.com/agent/tickets/{zend_ticket['id']}"
                ticket_response['subject'] = subject
                ticket_response['tags'] = zend_ticket['tags']
                ticket_response['status'] = zend_ticket['status']
                ticket_response['description'] = description
                category = ticket_response.get('category', 'Unknown')
                if category == 'Purchase':
                    category = 'New $$$'
                ticket_response['category'] = category
                if category not in self.categories:
                    self.categories[category] = []
                self.categories[category].append(ticket_response)

                self.zendesk.auto_reply(ticket_response)
                if answer:
                    ticket_response['answer'] = (
                        f"<b>Original Answer</b>: {answer}.<br> <b>Gen Answer</b>: {ticket_response['answer']}"
                    )
                self.all_tickets.append(ticket_response)

        report_name = f"daily_{Zendesk.tickets_view}_{current_time}_{knowedge_id}_{LLM.MODEL_NAME.replace('/', '-').replace(':', '-')}"
        end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        categories_json_object = {cat: len(tickets) for cat, tickets in self.categories.items()}
        ticket_ids = [ticket['id'] for ticket in self.all_tickets if ticket.get('category') != "Error"]
        run_data = {
            "filename": report_name + ".json",
            "tickets": Zendesk.tickets_view,
            "rag": Knowledge.is_rag,
            "knowledge": knowedge_id,
            "currentTime": current_time,
            "startTime": start_time,
            "endTime": end_time,
            "ticketIds": ticket_ids,
            "ticketsSize": len(self.all_tickets),
            "model": LLM.MODEL_NAME,
            "model_full_name": LLM.MODEL_FULL_NAME,
            "categories": categories_json_object,
        }
        try:
            with open("runs.json", "r") as f:
                runs_data = json.load(f)
        except FileNotFoundError:
            runs_data = []

        runs_data.append(run_data)
        with open("runs.json", "w") as f:
            json.dump(runs_data, f, indent=4)
        with open(report_name + ".json", "w", encoding="utf-8") as f:
            json.dump(self.all_tickets, f)


if __name__ == "__main__":
    app = Report()
    app.run()
