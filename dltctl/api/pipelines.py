from cmath import pi
import json, time
import click
import datetime
from databricks_cli.pipelines.api import PipelinesApi
from dltctl.types.events import PipelineEventsResponse

class PipelineNameNotUniqueError(Exception):
    pass

class PipelineNameNotFoundError(Exception):
    pass

class PipelinesApi(PipelinesApi):
    
    def get_pipeline_id_by_name(self, pipeline_name):
        pipelines = self.list()
        if(len(pipelines) < 1):
            return None
        
        pipelines_with_name = []
        for pipeline in pipelines:
            if pipeline["name"] == pipeline_name:
                pipelines_with_name.append(pipeline)
        
        if(len(pipelines_with_name) < 1):
            return None
        elif (len(pipelines_with_name) > 1):
            raise PipelineNameNotUniqueError(f"Unable to get pipeline by name: Multiple pipelines with the same name: {pipelines_with_name} ")
        else:
            return pipelines_with_name[0]["pipeline_id"]

    def get_last_update_id(self, pipeline_id):
        pipeline = self.get(pipeline_id)
        if "latest_updates" in pipeline:
            last_update_id = pipeline["latest_updates"][0]["update_id"]
            return last_update_id
        else:
            raise Exception("Pipeline exists but has no updates. It has likely never ran")

    def get_last_update(self, pipeline_id):
        pipeline = self.get(pipeline_id)
        if "latest_updates" in pipeline:
            last_update = pipeline["latest_updates"][0]["update_id"]
            return self.client.client.perform_query('GET', f'/pipelines/{pipeline_id}/updates/{last_update}')
        else:
            raise Exception("Pipeline exist but has no updates. It has likely never ran")
    
    def get_pipeline_settings(self, pipeline_id):
        pipeline = self.get(pipeline_id)
        return pipeline["spec"]

    def get_pipeline_state(self, pipeline_id):
        pipeline = self.get(pipeline_id)
        return pipeline["state"]

    def get_pipeline_libraries(self, pipeline_id):
        libs = []
        pipeline = self.get(pipeline_id)
        libraries = pipeline["spec"]["libraries"]
        for library in libraries:
            libs.append(library["notebook"]["path"])
        return libs
        
    def stream_events(self, pipeline_id, ts=None, polling_interval=3, max_polls_without_events=None, verbose=False):
        start_time = ts
        polls_without_events = 0
        last_event = None
        while True:
          to_exit = False
          t = (datetime.datetime.utcnow() - datetime.timedelta(seconds=5)).isoformat()[:-3]+'Z'  if start_time is None else start_time
          time.sleep(polling_interval)
          json_events = self.get_events(pipeline_id, max_result=100, timestamp_filter=t)
          events = PipelineEventsResponse().from_json_response(json_events).to_pipeline_events()
          if events is None:
              polls_without_events+=1
              if not last_event:
                  continue
              elif 'WAITING_FOR_RESOURCES' in last_event.message:
                  polls_without_events = 0 

              if max_polls_without_events and (polls_without_events >= max_polls_without_events):
                  break
              else:
                  continue
          polls_without_events = 0
          start_time = events[-1].timestamp
          for event in events:
              last_event = event
              color = 'red' if (event.level == 'ERROR' or event.error) else 'green'
              emoji = u'\u2714'
              click.secho(emoji + " ", fg=color, nl=False)
              click.secho(event.timestamp + " ", nl=False)
              click.secho(event.event_type + " ", nl=False, fg=color)
              click.secho(event.message)
              if verbose:
                  click.echo("Verbose Details:", nl=True)
                  click.secho(event.details)
              if 'update_progress' in event.details and event.details["update_progress"]["state"] == 'COMPLETED':
                  click.secho("Pipeline execution is complete", fg=color)
                  print("")
                  to_exit = True

              elif event.error:
                  try:
                      message = event.error["exceptions"][0]["message"]
                  except: 
                      message = event.error["exceptions"]

                  click.secho("Pipeline execution has FAILED. Failure reason:", fg=color)
                  click.secho(f"{message}", fg=color)
                  print("")
                  to_exit = True
              
          if to_exit:
              break
 
    def create(self,settings,headers=None):
        data = settings
        return self.client.client.perform_query('POST', '/pipelines', data=data,
                                                headers=headers)
                                                
    def edit(self, pipeline_id, settings, headers=None):
        data = settings.to_json()
        data["id"] = pipeline_id
        self.client.client.perform_query('PUT', '/pipelines/{}'.format(pipeline_id), data=data,
                                         headers=headers)

    def stop(self, pipeline_id, headers=None):
        self.client.stop(pipeline_id, headers)
        status = 'RUNNING'
        while(status == 'RUNNING'):
            status = self.get(pipeline_id)["state"]
            time.sleep(3)
    
    def stop_async(self, pipeline_id, headers=None):
        self.client.stop(pipeline_id, headers)
        return

    def get_events(self, pipeline_id, max_result=100, order_by="timestamp asc", timestamp_filter=None):
        _data = {}
        _data["max_results"] = max_result
        _data["order_by"] = order_by
        if timestamp_filter:
            _data["filter"] = f'timestamp > \'{timestamp_filter}\''

        response = self.client.client.perform_query(
            'GET', f'/pipelines/{pipeline_id}/events', data=_data)
        
        return response
    