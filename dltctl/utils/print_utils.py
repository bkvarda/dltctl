import click
import datetime

def event_print(type, level, msg):
    ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    color = 'green'
    if level == 'WARNING':
        color = 'yellow'
    if level == 'ERROR':
        color = 'red'
        
    emoji = u'\u2714'
    click.secho(emoji + " ", fg=color, nl=False)
    click.secho(ts + " ", nl=False)
    click.secho(type + " ", nl=False, fg=color)
    click.secho(msg)
    return