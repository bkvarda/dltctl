import json

class PipelineEvent:
    def __init__(self):
        self.id = None
        self.sequence = None
        self.origin = None
        self.timestamp = None
        self.message = None
        self.level = None
        self.details = None
        self.event_type = None
    
    def from_json(self, event):
        json_event = json.loads(event)
        self.id = json_event["id"] if "id" in json_event else None
        self.origin = json_event["origin"] if "origin" in json_event else None
        self.sequence = json_event["sequence"] if "sequence" in json_event else None
        self.timestamp = json_event["timestamp"] if "timestamp" in json_event else None
        self.message = json_event["message"] if "message" in json_event else None
        self.level = json_event["level"] if "level" in json_event else None
        self.error = json_event["error"] if "error" in json_event else None
        self.details = json_event["details"] if "details" in json_event else None
        self.event_type = json_event["event_type"] if "event_type" in json_event else None
        return self
    
class PipelineEventsResponse:
    def __init__(self):
     self.events = None
     self.next_page_token = None
     self.previous_page_token = None
     self.pipeline_events = []

    def from_json_response(self, json_response):
      self.events = json_response["events_json"] if "events_json" in json_response else None
      self.next_page_token = json_response["next_page_token"] if "next_page_token" in json_response else None
      self.previous_page_token = json_response["prev_page_token"] if "prev_page_token" in json_response else None
      return self
    
    def to_pipeline_events(self):
        if self.events is None:
            return
        else:
            pipeline_events = []
            for event in self.events:
                e = PipelineEvent().from_json(event)
                pipeline_events.append(e)
            self.pipeline_events = pipeline_events
            return pipeline_events

