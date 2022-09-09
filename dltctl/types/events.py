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
    
    def to_typed_event(self):
        if self.event_type == "flow_progress":
            self.__class__ = PipelineFlowProgressEvent
            return self
        elif self.event_type == "create_update":
            self.__class__ = PipelineCreateUpdateEvent
            return self
        elif self.event_type == "update_progress":
            self.__class__ = PipelineUpdateProgressEvent
            return self
        elif self.event_type == "flow_definition":
            self.__class__ = PipelineFlowDefinitionEvent
            return self
        elif self.event_type == "dataset_definition":
            self.__class__ = PipelineDatasetDefinitionEvent
            return self
        elif self.event_type == "graph_created":
            self.__class__ = PipelineGraphCreatedEvent
            return self
        elif self.event_type == "maintenance_progress":
            self.__class__ = PipelineMaintenanceProgressEvent
            return self
        else:
            return self

class PipelineDatasetDefinitionEvent(PipelineEvent):
    def __init__(self, *args, **kwargs):
        super(PipelineEvent, self).__init__(*args, **kwargs)

class PipelineGraphCreatedEvent(PipelineEvent):
    def __init__(self):
        super(PipelineEvent, self).__init__(*args, **kwargs)

class PipelineUserActionEvent(PipelineEvent):
    def __init__(self, *args, **kwargs):
        super(PipelineEvent, self).__init__(*args, **kwargs)

class PipelineFlowDefinitionEvent(PipelineEvent):
    def __init__(self, *args, **kwargs):
        super(PipelineEvent, self).__init__(*args, **kwargs)

class PipelineFlowProgressEvent(PipelineEvent):
    def __init__(self, *args, **kwargs):
        super(PipelineEvent, self).__init__(*args, **kwargs)

class PipelineAuditEvent(PipelineEvent):
    def __init__(self, *args, **kwargs):
        super(PipelineEvent, self).__init__(*args, **kwargs)

class PipelineCreateUpdateEvent(PipelineEvent):
    def __init__(self, *args, **kwargs):
        super(PipelineEvent, self).__init__(*args, **kwargs)

class PipelineUpdateProgressEvent(PipelineEvent):
    def __init__(self,  *args, **kwargs):
        super(PipelineEvent, self).__init__(*args, **kwargs)

class PipelineMaintenanceProgressEvent(PipelineEvent):
    def __init__(self, *args, **kwargs):
        super(PipelineEvent, self).__init__(*args, **kwargs)


class PipelineEventsResponse:
    def __init__(self):
     self.events = None
     self.next_page_token = None
     self.previous_page_token = None
     self.pipeline_events = []
     self.typed_pipeline_events = {
         "flow_progress":[],
         "create_update":[],
         "update_progress": [],
         "flow_definition": [],
         "dataset_definition": [],
         "maintenance_progress": [],
         "graph_created":[],
         "other": []
     }

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
    
    def to_typed_pipeline_events(self):
        typed_pipeline_events = {
         "flow_progress":[],
         "create_update":[],
         "update_progress": [],
         "flow_definition": [],
         "dataset_definition": [],
         "graph_created":[],
         "maintenance_progress":[],
         "other": []
     }
        if self.events is None:
            return
        else:
            pipeline_events = []
            for event in self.events:
                e = PipelineEvent().from_json(event)
                pipeline_events.append(e)
                if e.event_type in typed_pipeline_events.keys():
                    typed_pipeline_events[e.event_type].append(e.to_typed_event())
                else:
                    typed_pipeline_events["other"].append(e)

            self.typed_pipeline_events = typed_pipeline_events
            return typed_pipeline_events

